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

type routerByHTTPUser map[string][]*Router

type Routers struct {
	indexByDomain map[string]routerByHTTPUser
	enabledDomain map[string]routerByHTTPUser // A subset of indexByDomain

	mutex sync.RWMutex

	client *redis.Client
	pubsub *redis.PubSub
}

type Router struct {
	domain   string
	location string
	httpUser string

	// store any object here
	payload interface{}
}

func (r *Routers) initClient() {
	r.client = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("RD_HOST") + ":6379",
		Password: "",
		DB:       0,
	})

	var err error
	for {
		_, err = r.client.Ping().Result()
		if err != nil {
			fmt.Printf("connectRedis : %s\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	_, err = r.client.Do("CONFIG", "SET", "notify-keyspace-events", "Eh").Result()
	if err != nil {
		fmt.Printf("unable to set keyspace events %v", err.Error())
		os.Exit(1)
	}

	r.pubsub = r.client.PSubscribe("__keyevent@0__:hset*")
	fmt.Println("Finish Subscribe")
}

func (r *Routers) RedisListen() {
	for { // infinite loop
		fmt.Println("RedisListen Loop")
		message, err := r.pubsub.ReceiveMessage()
		if err != nil {
			fmt.Printf("error message - %v", err.Error())
			break
		}
		fmt.Printf("Keyspace event recieved %v  \n", message.String())
		redisKey := message.Payload
		colonIdx := strings.Index(redisKey, ":")
		domain := redisKey[colonIdx+1:]
		ret := r.recheck(domain)
		fmt.Printf("Recheck domain %v -> %v \n", domain, ret)
	}
}

func (r *Routers) filter(domain string) bool {
	if r.client == nil {
		return true
	}
	hGet := r.client.HGet("subdomain:"+domain, "enabled")
	if hGet.Val() == "T" {
		fmt.Println("RouterFilter : ", domain, " -> ", hGet.Val())
		return true
	}
	fmt.Println("Filter : ", domain, " -> ", hGet.Val())
	return false
}

func (r *Routers) Enable(domain string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	_, found := r.enabledDomain[domain]
	if found {
		return false
	}

	r.enabledDomain[domain] = r.indexByDomain[domain]
	return true
}

func (r *Routers) Disable(domain string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	_, found := r.enabledDomain[domain]
	if !found {
		return false
	}

	delete(r.enabledDomain, domain)
	return true
}

func (r *Routers) recheck(domain string) bool {
	if r.filter(domain) {
		r.Enable(domain)
		return true
	} else {
		r.Disable(domain)
		return false
	}
}

func NewRouters() *Routers {
	ret := &Routers{
		indexByDomain: make(map[string]routerByHTTPUser),
		enabledDomain: make(map[string]routerByHTTPUser),
	}
	ret.initClient()
	go ret.RedisListen()
	return ret
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

	if r.filter(domain) {
		r.enabledDomain[domain] = routersByHTTPUser
	}

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
	if len(newVrs) == 0 && len(routersByHTTPUser) == 1 {
		delete(r.indexByDomain, domain)
		delete(r.enabledDomain, domain)
	} else {
		routersByHTTPUser[httpUser] = newVrs
	}
}

func (r *Routers) Get(host, path, httpUser string) (vr *Router, exist bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	// fmt.Println("H( 1 ) ", r.indexByDomain, host)
	routersByHTTPUser, found := r.enabledDomain[host]
	if !found {
		return
	}

	vrs, found := routersByHTTPUser[httpUser]
	// fmt.Println("H( 2 ) ", vrs, found)
	if !found {
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
