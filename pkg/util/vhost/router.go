package vhost

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

var (
	ErrRouterConfigConflict = errors.New("router config conflict")
)

type RouterFilter struct {
	FilterTable  map[string]bool
	FilterClient *redis.Client
}

func NewRouterFilter() *RouterFilter {
	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("RD_HOST") + ":6379",
		Password: "",
		DB:       0,
	})

	var err error
	for {
		_, err = client.Ping().Result()
		if err != nil {
			fmt.Printf("connectRedis : %s\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	return &RouterFilter{
		FilterTable:  make(map[string]bool),
		FilterClient: client,
	}
}

func (rf *RouterFilter) Filter(host string) bool {
	if rf.FilterClient == nil {
		return true
	}
	hGet := rf.FilterClient.HGet("subdomain:"+host, "enabled")
	if hGet.Val() == "T" {
		fmt.Println("RouterFilter : ", host, " -> ", hGet.Val())
		return true
	}
	fmt.Println("RouterFilter : ", host, " -> ", hGet.Val())
	return false
}

type routerByHTTPUser map[string][]*Router

type Routers struct {
	indexByDomain map[string]routerByHTTPUser
	RouterFilter  RouterFilter

	mutex sync.RWMutex
}

type Router struct {
	domain   string
	location string
	httpUser string

	// store any object here
	payload interface{}
}

func NewRouters() *Routers {
	return &Routers{
		indexByDomain: make(map[string]routerByHTTPUser),
		RouterFilter:  *NewRouterFilter(),
	}
}

func (r *Routers) Add(domain, location, httpUser string, payload interface{}) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exist := r.exist(domain, location, httpUser); exist {
		return ErrRouterConfigConflict
	}

	routersByHTTPUser, found := r.indexByDomain[domain]
	if !found {
		routersByHTTPUser = make(map[string][]*Router)
	}
	vrs, found := routersByHTTPUser[httpUser]
	if !found {
		vrs = make([]*Router, 0, 1)
	}

	vr := &Router{
		domain:   domain,
		location: location,
		httpUser: httpUser,
		payload:  payload,
	}
	vrs = append(vrs, vr)
	sort.Sort(sort.Reverse(ByLocation(vrs)))

	routersByHTTPUser[httpUser] = vrs
	r.indexByDomain[domain] = routersByHTTPUser
	return nil
}

func (r *Routers) Del(domain, location, httpUser string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	routersByHTTPUser, found := r.indexByDomain[domain]
	if !found {
		return
	}

	vrs, found := routersByHTTPUser[httpUser]
	if !found {
		return
	}
	newVrs := make([]*Router, 0)
	for _, vr := range vrs {
		if vr.location != location {
			newVrs = append(newVrs, vr)
		}
	}
	routersByHTTPUser[httpUser] = newVrs
}

func (r *Routers) Get(host, path, httpUser string) (vr *Router, exist bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	// fmt.Println("H( 1 ) ", r.indexByDomain, host)
	routersByHTTPUser, found := r.indexByDomain[host]
	if !found {
		return
	}

	vrs, found := routersByHTTPUser[httpUser]
	// fmt.Println("H( 2 ) ", vrs, found)
	if !found {
		return
	}
	if !r.RouterFilter.Filter(host) {
		return
	}

	// can't support load balance, will to do
	for _, vr = range vrs {
		if strings.HasPrefix(path, vr.location) {
			return vr, true
		}
	}
	return
}

func (r *Routers) exist(host, path, httpUser string) (route *Router, exist bool) {
	routersByHTTPUser, found := r.indexByDomain[host]
	if !found {
		return
	}
	routers, found := routersByHTTPUser[httpUser]
	if !found {
		return
	}

	for _, route = range routers {
		if path == route.location {
			return route, true
		}
	}
	return
}

// sort by location
type ByLocation []*Router

func (a ByLocation) Len() int {
	return len(a)
}
func (a ByLocation) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByLocation) Less(i, j int) bool {
	return strings.Compare(a[i].location, a[j].location) < 0
}
