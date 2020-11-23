package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var nodes [3]net.Conn
var value int
var seqInt = 90

var completeNodeinfo [2] NodeInfo

type NodeInfo struct {
	NodeId     int    `json:"nodeId"`
	NodeIpAddr string `json:"nodeIpAddr"`
	Port       string `json:"port"`
}

/* A standard format for a Request/Response for adding node to cluster */
type AddToClusterMessage struct {
	Source  NodeInfo `json:"source"`
	Dest    NodeInfo `json:"dest"`
	Message string   `json:"message"`
}
type Message struct {
	MessageType  string `json:"MessageType"`
	Request      permissionRequest `json:"Request"`
	Response     permissionGranted `json:"Response"`
	NacKMess     nack	`json:"NackMess"`
	SuggestValue suggestion `json:"SuggestValue"`
	AcceptMes    accept	`json:"AcceptMes"`
}
type permissionRequest struct {
	Sid int `json:"Sid"`
}
type permissionGranted struct {
	S_id_of_permission_request int
	Last_accepted_s_id         int
	Last_accepted_value        int
}

type suggestion struct {
	S_id_of_permission_request int
	Value                      int
}
type nack struct {
	Promised_id int
}
type accept struct {
	S_id_permission_request int
}

func (node NodeInfo) String() string {
	return "NodeInfo:{ nodeId:" + strconv.Itoa(node.NodeId) + ", nodeIpAddr:" + node.NodeIpAddr + ", port:" + node.Port + " }"
}
func (req AddToClusterMessage) String() string {
	return "AddToClusterMessage:{\n  source:" + req.Source.String() + ",\n  dest: " + req.Dest.String() + ",\n  message:" + req.Message + " }"
}
func main() {
	makeMasterOnError := flag.Bool("makeMasterOnError", false, "make this node master if unable to connect to the cluster ip provided.")
	clusterip := flag.String("clusterip", "127.0.0.1:8001", "ip address of any node to connnect")
	myport := flag.String("myport", "8001", "ip address to run this node on. default is 8001.")
	flag.Parse()

	rand.Seed(time.Now().UTC().UnixNano())
	myid := rand.Intn(99999999)
	value = myid
	myIp, _ := net.InterfaceAddrs()
	me := NodeInfo{NodeId: myid, NodeIpAddr: myIp[0].String(), Port: *myport}
	dest := NodeInfo{NodeId: -1, NodeIpAddr: strings.Split(*clusterip, ":")[0], Port: strings.Split(*clusterip, ":")[1]}
	fmt.Println("My details:", me.String())

	ableToConnect := connectToCluster(me, dest)

	if ableToConnect || (!ableToConnect && *makeMasterOnError) {
		if *makeMasterOnError {
			fmt.Println("Will start this node as master.")
			go propose()
			go reciveiPromises()
		}
		listenOnPort(me)
	} else {
		fmt.Println("Quitting system. Set makeMasterOnError flag to make the node master.", myid)
	}
}


func getAddToClusterMessage(source NodeInfo, dest NodeInfo) AddToClusterMessage {
	return AddToClusterMessage{
		Source: NodeInfo{
			NodeId:     source.NodeId,
			NodeIpAddr: source.NodeIpAddr,
			Port:       source.Port,
		},
		Dest: NodeInfo{
			NodeId:     dest.NodeId,
			NodeIpAddr: dest.NodeIpAddr,
			Port:       dest.Port,
		},
	}
}

func connectToCluster(me NodeInfo, dest NodeInfo) bool {
	/* connect to this socket details provided */
	connOut, err := net.DialTimeout("tcp", dest.NodeIpAddr+":"+dest.Port, time.Duration(10)*time.Second)

	if err != nil {
		if _, ok := err.(net.Error); ok {
			fmt.Println("Couldn't connect to cluster.", me.NodeId)
			return false
		}
	} else {
		fmt.Println("Connected to cluster. Sending message to node.")

		requestMessage := getAddToClusterMessage(me, dest)
		json.NewEncoder(connOut).Encode(&requestMessage)

		decoder := json.NewDecoder(connOut)
		var responseMessage AddToClusterMessage

		decoder.Decode(&responseMessage)
		fmt.Println("Got Response:\n" + responseMessage.String())
		//for {
		//	decoder.Decode(&nodes)

		//	}
		fmt.Println(value)
		go acceptor(connOut, value)
		return true
	}
	return false
}

func listenOnPort(me NodeInfo) {
	/* Listen for incoming messages */
	ln, _ := net.Listen("tcp", fmt.Sprint(":"+me.Port))
	/* accept connection on port */
	/* not sure if looping infinetely on ln.Accept() is good idea */
	var i int = 0

	for {

		connIn, err := ln.Accept()
		fmt.Println(reflect.TypeOf(connIn))
		if err != nil {
			if _, ok := err.(net.Error); ok {
				fmt.Println("Error received while listening.", me.NodeId)
			}
		} else {

			var requestMessage AddToClusterMessage
			json.NewDecoder(connIn).Decode(&requestMessage)
			fmt.Println("Got Request:\n" + requestMessage.String())

			nodes[i] = connIn
			completeNodeinfo[i] = requestMessage.Source
			i++

			responseMessage := getAddToClusterMessage(me, requestMessage.Source)
			json.NewEncoder(connIn).Encode(&responseMessage)

			//for k := 0; k < i; k++ {
			//	json.NewEncoder(nodes[k]).Encode(&nodes[k])
			//}

			//connIn.Close()
		}
	}

}
func propose() {
	//sending proposal

	//reader :=bufio.NewReader(os.Stdin)
	//reader.ReadString('\n')
	for {
		//fmt.Println("hello propose")

		var prepareMessage Message = Message{
			MessageType:  "prepare",
			Request:      permissionRequest{seqInt},
			Response:     permissionGranted{},
			NacKMess:     nack{},
			SuggestValue: suggestion{},
			AcceptMes:    accept{},

		}
		//var j string="look";
		for _, s := range nodes {
			if s != nil {
				json.NewEncoder(s).Encode(&prepareMessage)
				fmt.Println(prepareMessage)
			}
		}
	}
}
func reciveiPromises() {
	//receiving promises
	var acceptedNode int=0
	for {
		fmt.Println(value)

		var responseMessage Message
		var promisedNodesNo int
		for _, s := range nodes {
			if s != nil {
				json.NewDecoder(s).Decode(&responseMessage)

				switch responseMessage.MessageType {
				case "promise":
					promisedNodesNo++
					fmt.Println("Reciving Promise",promisedNodesNo,"jjj",len(nodes)/2)

					if promisedNodesNo <=(len(nodes) / 2) {
						suggestAvalue(responseMessage.Response.S_id_of_permission_request, value)
						fmt.Println("hello promises")
						break
					}
				case "nack":
					fmt.Println("Reciving Nack")
					if responseMessage.NacKMess.Promised_id > seqInt {
						fmt.Println("hererere")
					//	seqInt = responseMessage.NacKMess.Promised_id  +1
					}
				case "accept":
						responseMessage =Message{
							MessageType:  "learn",
							Request:      permissionRequest{},
							Response:     permissionGranted{},
							NacKMess:     nack{},
							SuggestValue: suggestion{
							Value:value ,
							},
							AcceptMes:    accept{},
						}
					    acceptedNode++
					    if(acceptedNode<(len(nodes)/2)){
					        for _, s := range nodes {
					    	//	fmt.Println(suggestionMessage)
					    		if(s!=nil) {
					    			json.NewEncoder(s).Encode(&responseMessage)
					    		}
					    	}

						}
				}
			}
		}
	}
}
func suggestAvalue(s_id int, newValue int) {
	var suggestionMessage Message
	suggestionMessage = Message{
		MessageType: "sugg",
		Request:     permissionRequest{},
		Response:    permissionGranted{},
		NacKMess:    nack{},
		SuggestValue: suggestion{
			S_id_of_permission_request: s_id,
			Value:                      newValue,
		},
	}
	for _, s := range nodes {
		fmt.Println(suggestionMessage)
		if(s!=nil) {
			json.NewEncoder(s).Encode(&suggestionMessage)
		}
	}
}
func acceptor(connOut net.Conn, v int) {

	var promisedSid int = -1
	for {
		var requestMessage Message
		var responseMessage Message
		//var j string;
		decoder := json.NewDecoder(connOut)
		decoder.Decode(&requestMessage)
		//handling prepare
	//	fmt.Println(requestMessage.MessageType)
	fmt.Println(value)
		switch requestMessage.MessageType {

		case "prepare":
			if promisedSid == -1 || promisedSid < requestMessage.Request.Sid {

				responseMessage = Message{
					MessageType: "promise",
					Request:     permissionRequest{},
					Response: permissionGranted{
						responseMessage.Request.Sid,
						2,
						value,
					},
				}
			//	promisedSid=requestMessage.Request.Sid
			} else {
				responseMessage = Message{
					MessageType: "nack",
					Request:     permissionRequest{},
					Response:    permissionGranted{},
					NacKMess: nack{
						Promised_id: promisedSid,
					},
				}
			}
			json.NewEncoder(connOut).Encode(&responseMessage)
		case "sugg":
			fmt.Println("recivied a suggestion")
			responseMessage = Message{
				MessageType:  "accept",
				Request:      permissionRequest{},
				Response:     permissionGranted{},
				NacKMess:     nack{},
				SuggestValue: suggestion{},
				AcceptMes: accept{
					requestMessage.Request.Sid,
				},
			}
			if promisedSid < requestMessage.SuggestValue.S_id_of_permission_request {
				json.NewEncoder(connOut).Encode(&responseMessage)
				value=requestMessage.SuggestValue.Value
			} else {
				responseMessage = Message{
					MessageType: "nack",
					Request:     permissionRequest{},
					Response:    permissionGranted{},
					NacKMess: nack{
						Promised_id: promisedSid,
					},
				}
				json.NewEncoder(connOut).Encode(&responseMessage)
			}

		}

	}
}
func learner(connOut net.Conn) {
	var requestMessage Message
	decoder := json.NewDecoder(connOut)
	decoder.Decode(&requestMessage)
	switch requestMessage.MessageType {
	case "learn":
		value=requestMessage.SuggestValue.Value
	}

	}

