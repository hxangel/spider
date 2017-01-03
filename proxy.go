package spider

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
	"encoding/json"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"math/rand"
	"sync"
)

type ProxyServerInfo struct {
	Host    string
	Rate    float64 //network speed
	Region  string  //region
	Type    int     //1 http 2 https 3 socket
	Status  bool    //region
	Level   uint8   //0 transparent 1 low 2 high
	Count   uint8
	Tao     bool
	Jd      bool
	Hui     bool
	Created time.Time
	Checked time.Time
}

type Checker struct {
	Name       string
	Collection string
	Url        string
	MatchStr   string
}

//
//ip:port: "121.232.144.143:9000",
//ptype: "HTTP",
//anonymous: "高匿",
//country: "CN",
//elapsed: "0.13862101700000000000",
//upcount: "1",
//last_check_time: "2016-11-30 20:43:16"

type Proxy struct {
	Rows map[uint32]*ProxyServerInfo
	Chk  []*ProxyServerInfo
	Tao  []*ProxyServerInfo
	Jd   []*ProxyServerInfo
	Hui  []*ProxyServerInfo
	Now  map[string][]*ProxyServerInfo
	Last time.Time
}

const TimeFormat = "2006-01-02 15:04:05"
const TimeOut = time.Second * 10
const Worker = 80

var (
	SpiderProxy *Proxy
	proxyTebiereUrl = "http://proxy.tebiere.com/api/fetch/list?key=OxTNiiS9PjlWIDD1KEgU71ZjZQHNxh&num=10000&port=&check_country_group%5B0%5D=1&check_http_type%5B0%5D=0&check_anonymous%5B0%5D=3&check_elapsed=2&check_upcount=500&result_sort_field=1&check_result_fields%5B0%5D=2&check_result_fields%5B1%5D=3&check_result_fields%5B2%5D=4&check_result_fields%5B3%5D=5&check_result_fields%5B4%5D=6&check_result_fields%5B5%5D=7&result_format=json"
	proxy66ipMoUrl = "http://www.66ip.cn/mo.php?sxb=&tqsl=1000&ports%5&jdfwkey=xozpl3";
	proxy66ipNmtqUrl = "http://www.66ip.cn/nmtq.php?getnum=1000&anonymoustype=%s&proxytype=2&api=66ip";
	ProxyBaseTableName = "proxy_base"
	count = 0
	JdChecker = Checker{Name:"Jd", Collection:"proxy_jd", Url:"https://www.jd.com/intro/about.aspx", MatchStr:"070359"}
	TaoChecker = Checker{Name:"Tao", Collection:"proxy_tao", Url:"https://err.taobao.com/error1.html", MatchStr:"alibaba.com"}
	HuiChecker = Checker{Name:"Hui", Collection:"proxy_hui", Url:"http://www.huihui.cn/intro/join_us", MatchStr:"080268"}
	Checking bool
)

func (sp *Proxy) MgoSession(collections string) (c *mgo.Collection) {
	if collections == "" {
		collections = ProxyBaseTableName
	}

	//str := "mongodb://10.162.88.159:27017"
	str := "mongodb://gouUserAdmin:Ger2_eFucku@10.162.88.159:27017/gou"
	session, err := mgo.Dial(str)
	if err != nil {
		panic(err)
	}
	//defer session.Close()
	c = session.DB("gou").C(collections)
	return
}
func NewProxy() *Proxy {
	return &Proxy{}
}

func StartProxy() *Proxy {
	if SpiderProxy == nil {
		SpiderLoger.I("SpiderProxy Daemon.")
		SpiderProxy = NewProxy()
		SpiderProxy.Daemon()
	}
	return SpiderProxy
}
func Random(n int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(n);
}
func (sp *Proxy) Daemon() {
	//一个小时抓取一次
	tick_get := time.NewTicker(time.Hour)
	//两个小时候检查一次
	//tick_check := time.NewTicker(2 * time.Second)
	tick_check := time.NewTicker(8 * time.Hour)
	//同步阻塞
	go func() {
		go sp.Check(ProxyBaseTableName)
		for {
			select {
			case <-tick_get.C:
				sp.getTebiere(proxyTebiereUrl)
				sp.Get66ip(proxy66ipMoUrl);
				sp.Get66ip(proxy66ipNmtqUrl);
			case <-tick_check.C:
				go sp.Check(ProxyBaseTableName)
			}
		}

	}()
}
func (sp *Proxy) DelProxyServer(index uint32) {
	SpiderLoger.D("delete proxyserver", index)
	delete(sp.Rows, index)
}

func (sp *Proxy) GetProxyServer(Type string) *ProxyServerInfo {
	condition := bson.M{}
	count := len(sp.Now[Type])
	if count < 10 {
		var results []*ProxyServerInfo
		ms := sp.MgoSession(ProxyBaseTableName)
		if Type == "jd" {
			condition = bson.M{"jd":true}
		} else if Type == "tao" {
			condition = bson.M{"tao":true}
		} else if Type == "hui" {
			condition = bson.M{"hui":true}
		} else {
			return nil
		}

		err := ms.Find(condition).Limit(1000).Sort("-checked").All(&results);
		if err != nil {
			SpiderLoger.E("Spider.Check Mongo Find Err", err);
			return nil
		}
		if (len(results) == 0) {
			return nil
		}
		if sp.Now == nil {
			sp.Now = make(map[string][]*ProxyServerInfo, 3)
		}
		sp.Now[Type] = results
		count = len(sp.Now[Type])
	}
	i := Random(count - 1)
	info := sp.Now[Type][i]
	return info
}

func (sp *Proxy) Check(Collection string) {

	if Checking {
		return
	}
	Checking = true
	// Query All
	ms := sp.MgoSession(Collection)
	//mc := ms.Bulk()

	SpiderLoger.I("Start checking proxys")
	var (
		m sync.Mutex
		results []*ProxyServerInfo
		CheckedOK []*ProxyServerInfo
		//CheckedNO []*ProxyServerInfo
	)

	ch := make(chan bool, Worker)
	jobs := make(chan *ProxyServerInfo, Worker)
	finish := make(chan bool)
	done := 0
	ok := 0
	total := 0
	go func() {
		for {
			i, more := <-jobs
			if more {
				go func(i *ProxyServerInfo) {
					i.Check()
					m.Lock()
					CheckedOK = append(CheckedOK, i)
					if len(CheckedOK) >= 10 {
						sp.Upset(Collection, CheckedOK)
						CheckedOK = CheckedOK[:0]
					}
					ok++
					done++
					m.Unlock()
					<-ch
				}(i)
				//占坑
				ch <- true
			} else {
				time.Sleep(time.Second * 1)
				SpiderLoger.I("Nothing to Do && Waitting")
				//满号
				finish <- true
			}
		}

	}()
	//最后一次检测时间在8小时以前记录
	condition := bson.M{"checked": bson.M{"$lt": time.Now()}}

	for p := 0; ; p++ {
		err := ms.Find(condition).Skip(p * Worker).Limit(Worker).All(&results)

		if err != nil {
			SpiderLoger.E("Spider.Check Mongo Find Err", err);
		}
		//循环结束
		if len(results) == 0 {
			break
		}
		//分配任务
		for _, r := range results {
			//SpiderLoger.I("Each [", p, "-", i, "]")
			total += 1
			jobs <- r
		}
	}
	SpiderLoger.I("Now Waitting")
	for {
		if (total == done) {
			SpiderLoger.I("Total checked proxys [", done, "]")
			SpiderLoger.I("Total success proxys [", ok, "]")
			break
		}
		time.Sleep(TimeOut);
		SpiderLoger.I("Check Finish :", done)
		SpiderLoger.I("Check CurPlu :", total)
	}
	//<-finish
	//close(finish)
	//close(ch)
	//close(jobs)
	//sp.Upset(Collection, CheckedOK)
	//CheckedOK = CheckedOK[:0]
	Checking = false
	SpiderLoger.I("Check Finish OOOOOOOOPS")
	return
	//	fmt.Println("sent all jobs")
	//We await the worker using the synchronization approach we saw earlier.

}

func (sp *Proxy)Upset(Collection string, Rows []*ProxyServerInfo) {
	mc := sp.MgoSession(Collection).Bulk()
	for i, row := range Rows {
		type M map[string]string
		mc.Upsert(bson.M{"host":row.Host}, bson.M{"$set":row})
		if i % 200 == 0 && i > 1 {
			_, err := mc.Run()
			if err != nil {
				SpiderLoger.E("Proxy.Insert", "error performing upsert! error", err)
			}
			mc = sp.MgoSession(Collection).Bulk()
		}
	}
	SpiderLoger.I("Proxy.Upset", "update", len(sp.Chk), "rows")
	_, err := mc.Run()
	if err != nil {
		SpiderLoger.E("Proxy.Upset", "Insert new rocords error performing upsert! error", err)
	}
}
func (pi *ProxyServerInfo)Check() {
	pi.Checked = time.Now()
	pi.Tao = false
	pi.Hui = false
	pi.Jd = false
	if TaoChecker.Do(pi) {
		pi.Tao = true
		SpiderLoger.I("Check Tao OK [", pi.Host, "]")
	}
	if HuiChecker.Do(pi) {
		pi.Hui = true
		SpiderLoger.I("Check Hui OK [", pi.Host, "]")
	}
	if JdChecker.Do(pi) {
		pi.Jd = true
		SpiderLoger.I("Check JD  OK [", pi.Host, "]")
	}
	return
}
func (r Checker)Do(p *ProxyServerInfo) bool {

	var timeout = time.Duration(TimeOut)
	proxyUrl, _ := url.Parse(fmt.Sprintf("http://%s", p.Host))
	//	url_proxy := &url.URL{Host: host}
	client := &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(proxyUrl)},
		Timeout:   timeout}

	resp, err := client.Get(r.Url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false
	}
	body, _ := ioutil.ReadAll(resp.Body)
	if !strings.Contains(string(body), r.MatchStr) {
		return false
	}
	return true
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (sp *Proxy) Get66ip(proxyUrl string) {
	_, content, err := NewLoader().WithPcAgent().Get(proxyUrl)
	if err != nil {
		SpiderLoger.E("Load proxy error with", proxyUrl, err)
		return
	}

	m := make([]byte, len(content))
	copy(m, content)

	htmlParser := NewHtmlParser()

	hp := htmlParser.LoadData(m)
	trs := hp.Partten(`(?U)(\d+\.\d+\.\d+\.\d+\:\d+)<br/>`).FindAllSubmatch()
	l := len(trs)
	if l == 0 {
		SpiderLoger.E("load proxy data from " + proxyUrl + " error. ")
		return
	}
	count = len(sp.Rows)
	if count == 0 {
		sp.Rows = make(map[uint32]*ProxyServerInfo)
	}
	mc := sp.MgoSession("proxy_base").Bulk()

	for i, val := range trs {
		row := &ProxyServerInfo{}
		row.Host = string(val[1])
		//row.Type     = tmp["ptype"].(string)
		row.Type = 1
		//row.Region = tmp["country"].(string)
		//row.Level    = tmp["anonymous"].(string)
		row.Level = 2
		row.Created = time.Now()
		//row.Checked, _ = time.Parse(TimeFormat, tmp["last_check_time"].(string))

		type M map[string]string
		mc.Upsert(bson.M{"host":row.Host}, bson.M{"$set":row})
		//bulk.Upsert(M{"n": 4}, M{"$set": M{"n": 40}}, M{"n": 3}, M{"$set": M{"n": 30}})

		if i % 100 == 0 && i > 1 {
			_, err := mc.Run()
			if err != nil {
				SpiderLoger.E("Proxy.Insert", "error performing upsert! error", err)
			}

			mc = sp.MgoSession("proxy_base").Bulk()
		}

	}

	SpiderLoger.I("Proxy.Get66ip", "update", l, "rows")
	_, err = mc.Run()
	if err != nil {
		SpiderLoger.E("Proxy.Get66ip", "Insert new rocords error performing upsert! error", err)
	}

}

func (sp *Proxy) getKuai() {
	SpiderLoger.I("Proxy start new runtime with kuaidaili")
	for i := 1; i < 10; i++ {
		sp.kuai(fmt.Sprintf("http://www.kuaidaili.com/free/inha/%d/", i))
	}
}

func (sp *Proxy) kuai(proxyUrl string) {
	_, content, err := NewLoader().WithPcAgent().Get(proxyUrl)
	if err != nil {
		SpiderLoger.E("Load proxy error with", proxyUrl)
		return
	}

	m := make([]byte, len(content))
	copy(m, content)

	htmlParser := NewHtmlParser()

	hp := htmlParser.LoadData(m)
	trs := hp.Partten(`(?U)<td>(\d+\.\d+\.\d+\.\d+)</td>\s+<td>(\d+)</td>\s+<td>(.*)</td>`).FindAllSubmatch()
	l := len(trs)
	if l == 0 {
		SpiderLoger.E("load proxy data from " + proxyUrl + " error. ")
		return
	}
	count = len(sp.Rows)
	if count == 0 {
		sp.Rows = make(map[uint32]*ProxyServerInfo)
	}

	for i := 0; i < l; i++ {
		if string(trs[i][3]) != "高匿名" {
			continue
		}
		ip, port := string(trs[i][1]), string(trs[i][2])
		host := fmt.Sprintf("%s:%s", ip, port)
		h := hash(host)
		_, ok := sp.Rows[h]
		if ok {
			continue
		}
		go func() {
			sp.Rows[h] = &ProxyServerInfo{Host: host, Status: true}
			sp.Chk = append(sp.Chk, sp.Rows[h])
		}()
		count++
	}
	if count <= 5 {
		SpiderLoger.E("The proxy servers only ", count)
	}
	SpiderLoger.I("The proxy server count", count)
	return

}

func (sp *Proxy) getTebiere(proxyUrl string) {

	_, body, err := NewLoader().WithPcAgent().Get(proxyUrl)
	if err != nil {
		SpiderLoger.E("Proxy.GetApiProxyList", proxyUrl)
		return
	}

	result := make(map[string]interface{})
	err = json.Unmarshal(body, &result)
	if err != nil {
		SpiderLoger.E("[Proxy.GetApiProxyList]", err.Error())
		return
	}

	success, ok := result["success"].(bool);
	if !ok {
		SpiderLoger.E("[Proxy.GetApiProxyList] json parse error")
		return
	}
	if success == false {
		return
	}
	ipList, _ := result["list"].([]interface{})
	var Rows []*ProxyServerInfo
	for _, val := range ipList {
		row := &ProxyServerInfo{}
		tmp := val.(map[string]interface{})
		row.Host = tmp["ip:port"].(string)
		//row.Type     = tmp["ptype"].(string)
		row.Type = 1
		row.Region = tmp["country"].(string)
		//row.Level    = tmp["anonymous"].(string)
		row.Level = 2
		row.Created = time.Now()
		row.Checked, _ = time.Parse(TimeFormat, tmp["last_check_time"].(string))
		Rows = append(Rows, row)
		if (len(Rows) > 800) {
			Rows = Rows[:0]
			sp.Upset(ProxyBaseTableName, Rows);
		}
	}
	sp.Upset(ProxyBaseTableName, Rows);


}
