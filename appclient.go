package main  

import (  
    "fmt"  
    "sort"
    "net/http"
    "io/ioutil"
    "encoding/json"
    "hash/crc32"
)  

type DataSet struct{
        Key     int     `json:"key,omitempty"`
        Value   string  `json:"value,omitempty"`
}

type Instance struct {
        Id       int
        IP       string
}

type Shard []uint32

func CurrentInstance(id int, ip string) *Instance {
    return &Instance{  
        Id:       id,  
        IP:       ip,  
    }  
}  
  
type ConsistentHashingClient struct {
    Instances   map[uint32]Instance
    IsAlreadyThere map[int]bool
    Circle Shard
}


// Function to PUT KeyID and corresponding Value
func PutKeyIdValue(circle *ConsistentHashingClient, str string, input string){
    ip := circle.Get(str)
    address := "http://"+ ip.IP+"/keys/"+str+"/"+input
    fmt.Println(address)
    req,err := http.NewRequest("PUT",address,nil)
    client := &http.Client{}
    resp, err := client.Do(req)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer resp.Body.Close()
    }
}

// Function to GET Key value
func GetKeyId(key string,circle *ConsistentHashingClient){
    var out DataSet
    ip := circle.Get(key)
    address := "http://"+ ip.IP+"/keys/"+key
    fmt.Println(address)
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

// Function to GET all Values
func GetKeyIdValues(address string){
    var out []DataSet
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

func ConsistentHashingClientCurrent() *ConsistentHashingClient {
    return &ConsistentHashingClient{
        Instances:     make(map[uint32]Instance),
        IsAlreadyThere: make(map[int]bool),
        Circle:      Shard{},  
    }  
}  
  
func (hr *ConsistentHashingClient) AddInstance(node *Instance) bool {
    if _, ok := hr.IsAlreadyThere[node.Id]; ok {
        return false  
    }  
    str := hr.ReturnNodeIP(node)  
    hr.Instances[hr.GetHashValue(str)] = *(node)
    hr.IsAlreadyThere[node.Id] = true
    hr.ShardedHash()
    return true  
}

func (hr *ConsistentHashingClient) ReturnNodeIP(node *Instance) string {
    return node.IP
}

func (hr *ConsistentHashingClient) ShardedHash() {
    hr.Circle = Shard{}
    for k := range hr.Instances {
        hr.Circle = append(hr.Circle, k)
    }
    sort.Sort(hr.Circle)
}

  
func (hr *ConsistentHashingClient) Get(key string) Instance {
    hash := hr.GetHashValue(key)  
    i := hr.SearchForSharding(hash)
    return hr.Instances[hr.Circle[i]]
}

func (hr *ConsistentHashingClient) GetHashValue(key string) uint32 {
    return crc32.ChecksumIEEE([]byte(key))
}

func (hr *ConsistentHashingClient) SearchForSharding(hash uint32) int {
    i := sort.Search(len(hr.Circle), func(i int) bool {return hr.Circle[i] >= hash })  
    if i < len(hr.Circle) {  
        if i == len(hr.Circle)-1 {  
            return 0  
        } else {  
            return i  
        }  
    } else {  
        return len(hr.Circle) - 1  
    }  
}  


func (hr Shard) Len() int {
    return len(hr)
}

func (hr Shard) Less(i, j int) bool {
    return hr[i] < hr[j]
}

func (hr Shard) Swap(i, j int) {
    hr[i], hr[j] = hr[j], hr[i]
}

func main() {   
    circle := ConsistentHashingClientCurrent()
    circle.AddInstance(CurrentInstance(0, "127.0.0.1:3000"))
	circle.AddInstance(CurrentInstance(1, "127.0.0.1:3001"))
	circle.AddInstance(CurrentInstance(2, "127.0.0.1:3002"))
    PutKeyIdValue(circle,"1","a")
    PutKeyIdValue(circle,"2","b")
    PutKeyIdValue(circle,"3","c")
    PutKeyIdValue(circle,"4","d")
    PutKeyIdValue(circle,"5","e")
    PutKeyIdValue(circle,"6","f")
    PutKeyIdValue(circle,"7","g")
    PutKeyIdValue(circle,"8","h")
    PutKeyIdValue(circle,"9","i")
    PutKeyIdValue(circle,"10","j")
    GetKeyId("1",circle)
    GetKeyId("2",circle)
    GetKeyId("3",circle)
    GetKeyId("4",circle)
    GetKeyId("5",circle)
    GetKeyId("6",circle)
    GetKeyId("7",circle)
    GetKeyId("8",circle)
    GetKeyId("9",circle)
    GetKeyId("10",circle)
    GetKeyIdValues("http://127.0.0.1:3000/keys")
    GetKeyIdValues("http://127.0.0.1:3001/keys")
    GetKeyIdValues("http://127.0.0.1:3002/keys")
}  
