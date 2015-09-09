package main

import ( 
        "github.com/julienschmidt/httprouter"
        "github.com/unrolled/render"
        "gopkg.in/mgo.v2"
        "github.com/nicolasbarbe/kafka"
        "encoding/json" 
        "net/http"
        "time"
        "strconv"
        "log"
)


/** Types **/

// User represents a user in the system which can participate in a discussion
type DenormalizedUser struct {
  Id            string     `json:"id"            bson:"_id"`
  FirstName     string     `json:"firstName"     bson:"firstName"`
  LastName      string     `json:"lastName"      bson:"lastName"`
  MemberSince   time.Time  `json:"memberSince"   bson:"memberSince"`
}

type NormalizedUser struct {
  Id            string     `json:"id"            bson:"_id"`
  FirstName     string     `json:"firstName"     bson:"firstName"`
  LastName      string     `json:"lastName"      bson:"lastName"`
  MemberSince   time.Time  `json:"memberSince"   bson:"memberSince"`
}



// Controller embeds the logic of the microservice
type Controller struct {
  mongo         *mgo.Database
  producer      *kafka.Producer
  renderer      *render.Render
}

func (this *Controller) ListUsers(response http.ResponseWriter, request *http.Request, params httprouter.Params ) {
  var users []DenormalizedUser
  if err := this.mongo.C(usersCollection).Find(nil).Limit(100).All(&users) ; err != nil {
    log.Print(err)
  }

  this.renderer.JSON(response, http.StatusOK, users)
}

func (this *Controller) ShowUser(response http.ResponseWriter, request *http.Request, params httprouter.Params ) {

  var user DenormalizedUser
  if err := this.mongo.C(usersCollection).FindId(params.ByName("id")).One(&user) ; err != nil {
    this.renderer.JSON(response, http.StatusNotFound, "User not found")
    return
  }
   
  this.renderer.JSON(response, http.StatusOK, user)
}

func (this *Controller) ConsumeUsers(message []byte) {
  idx, _    := strconv.Atoi(string(message[:2]))
  eventType := string(message[2:idx+2])
  body      := message[idx+2:]

  if eventType != userCreated {
    log.Printf("Message with type %v is ignored. Type %v was expected", eventType, userCreated)
    return
  }

  // unmarshal user from event body
  var normalizedUser NormalizedUser
  if err := json.Unmarshal(body, &normalizedUser); err != nil {
    log.Print("Cannot unmarshal user")
    return
  }

  // create internal representation
  denormalizedUser := DenormalizedUser {
    Id          : normalizedUser.Id,            
    FirstName   : normalizedUser.FirstName,     
    LastName    : normalizedUser.LastName,      
    MemberSince : normalizedUser.MemberSince,   
  }

  // save user
  if err := this.mongo.C(usersCollection).Insert(denormalizedUser) ; err != nil {
    log.Printf("Cannot save document in collection %s : %s", usersCollection, err)
    return
  }
}

