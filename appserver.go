package main

import  (
	    "fmt"
	    "sort"
		"net/http"
		"strconv"
		"strings"
		"encoding/json"
		"github.com/julienschmidt/httprouter"
)

type DataSet struct{
		Key 	int		`json:"key,omitempty"`
		Value 	string	`json:"value,omitempty"`
} 


var serverInstance1, serverInstance2, serverInstance3 [] DataSet
var i1, i2, i3 int
type ByKey []DataSet


// Function to PUT the keyId and corresponding Value
func putKeyIdValue(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	pt := strings.Split(request.Host,":")
	key,_ := strconv.Atoi(p.ByName("key_id"))
	if(pt[1]=="3000"){
		serverInstance1 = append(serverInstance1, DataSet{key,p.ByName("value")})
		i1++
	}else if(pt[1]=="3001"){
		serverInstance2 = append(serverInstance2, DataSet{key,p.ByName("value")})
		i2++
	}else{
		serverInstance3 = append(serverInstance3, DataSet{key,p.ByName("value")})
		i3++
	}
}

// Function to GET the Value of a keyId
func getKeyId(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	output := serverInstance1
	index := i1
	pt := strings.Split(request.Host,":")
	if(pt[1]=="3001"){
		output = serverInstance2
		index = i2
	}else if(pt[1]=="3002"){
		output = serverInstance3
		index = i3
	}
	key,_ := strconv.Atoi(p.ByName("key_id"))
	for i:=0 ; i< index;i++{
		if(output[i].Key==key){
			result,_:= json.Marshal(output[i])
			fmt.Fprintln(rw,string(result))
		}
	}
}


// Function to GET the Values of all the Keys
func getKeys(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	pt := strings.Split(request.Host,":")
	if(pt[1]=="3000"){
		sort.Sort(ByKey(serverInstance1))
		result,_:= json.Marshal(serverInstance1)
		fmt.Fprintln(rw,string(result))
	}else if(pt[1]=="3001"){
		sort.Sort(ByKey(serverInstance2))
		result,_:= json.Marshal(serverInstance2)
		fmt.Fprintln(rw,string(result))
	}else{
		sort.Sort(ByKey(serverInstance3))
		result,_:= json.Marshal(serverInstance3)
		fmt.Fprintln(rw,string(result))
	}
}


func (a ByKey) Len() int{
	return len(a)
}
func (a ByKey) Swap(i, j int){
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

func main(){
	i1 = 0
	i2 = 0
	i3 = 0
	router := httprouter.New()
    router.GET("/keys", getKeys)
    router.GET("/keys/:key_id", getKeyId)
    router.PUT("/keys/:key_id/:value", putKeyIdValue)
	fmt.Println("Server running on port 3000")
    go http.ListenAndServe(":3000", router)
	fmt.Println("Server running on port 3001")
    go http.ListenAndServe(":3001", router)
	fmt.Println("Server running on port 3002")
    go http.ListenAndServe(":3002", router)
    select {}
}