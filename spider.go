package spider

import (
	"encoding/json"
	"fmt"
	utils "libs/utils"
	"net/url"
)

var (
	SpiderServer *Spider
	SpiderLoger  *MyLoger      = NewMyLoger()
	TryTime                    = 10
)

type Spider struct {
	qstart  chan *Item
	qfinish chan *Item
	qerror  chan *Item
}

type Item struct {
	loader *Loader
	htmlParse *HtmlParse
	params   map[string]string
	data     map[string]interface{}
	tag      string
	tryTimes int
	err      error
}

func NewSpider() *Spider {
	SpiderServer = &Spider{
		qstart:  make(chan *Item),
		qfinish: make(chan *Item),
		qerror:  make(chan *Item),
	}
	return SpiderServer
}

func Start() *Spider {
	if SpiderServer == nil {
		SpiderLoger.I("SpiderServer Daemon.")
		SpiderServer = NewSpider()
		SpiderServer.Daemon()
	}
	return SpiderServer
}

func (spider *Spider) Do(item *Item) {
	item.tryTimes++
	SpiderLoger.I(fmt.Sprintf("tag: <%s>, params: %v try with (%d) times.", item.tag, item.params, item.tryTimes))
	switch item.tag {
	case "TmallItem":
		ti := &Tmall{item: item}
		go ti.Item()
		break
	case "TaobaoItem":
		ti := &Taobao{item: item}
		go ti.Item()
		break
	case "JdItem":
		ti := &Jd{item: item}
		go ti.Item()
		break
	case "MmbItem":
		ti := &MMB{item: item}
		go ti.Item()
		break
	case "TmallShop":
		ti := &Tmall{item: item}
		go ti.Shop()
		break
	case "JdShop":
		ti := &Jd{item: item}
		go ti.Shop()
		break
	case "TaobaoShop":
		ti := &Taobao{item: item}
		go ti.Shop()
		break
	case "SameStyle":
		ti := &Taobao{item: item}
		go ti.SameStyle()
	case "Other":
		ti := &Other{item: item}
		go ti.Get()
		break
	}
	return
}

func (spider *Spider) Error(item *Item) {
	if item.err != nil {
		err := fmt.Sprintf("tag:<%s>, params: [%v] error :{%v}", item.tag, item.params["id"], item.err.Error())
		SpiderLoger.E(err)
		if item->tryTimes < 10 {
			SpiderServer.qstart<-item
		}
		item.err = nil
	}
	return
}

func (spider *Spider) Finish(item *Item) {
	output, err := json.Marshal(item.data)
	if err != nil {
		SpiderLoger.E("error with json output")
		return
	}
	v := url.Values{}
	v.Add("id", item.params["id"])
	v.Add("data", fmt.Sprintf("%s", output))
	SpiderLoger.D(v)
	url, _ := url.QueryUnescape(item.params["callback"])
	loader := NewLoader(url, "Post").WithProxy(false)
	_, err = loader.Send(v)
	if err != nil {
		SpiderLoger.E("Callback with error", err.Error())
		return
	}
	SpiderLoger.I("Success callback with", fmt.Sprintf("tag:<%s> params:%v", item.tag, item.params))
	item = nil
	return
}

func (spider *Spider) Add(tag string, params map[string]string) {
	item := &Item{
		htmlParse: NewHtmlParse(),
		tag:      tag,
		params:   params,
		tryTimes: 0,
		data:     make(map[string]interface{}),
		err:      nil,
	}
	spider.qstart <- item
	return
}

func (spider *Spider) Daemon() {
	go func() {
		for {
			select {
			case item := <-spider.qstart:
				go spider.Do(item)
				break
			case item := <-spider.qfinish:
				go spider.Finish(item)
				break
			case item := <-spider.qerror:
				go spider.Error(item)
				break
			}
		}
	}()
}
