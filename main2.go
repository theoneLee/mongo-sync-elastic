package main

import (
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	molog "mongo-sync-elastic/log"
	"mongo-sync-elastic/service"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	serviceSync()
}

// 全量更新后再增量更新,这个服务不会停
func serviceSync() {
	//init config
	filePath := "config.json"
	config, err := service.InitConfig(filePath)
	if err != nil {
		molog.ErrorLog.Printf("init config err:%v\n", err)
		return
	}
	molog.InfoLog.Printf(" init config success %+v \n", config)

	var oplogFile string
	if config.Tspath == "" {
		oplogFile = "./oplogts/" + config.MongoDB + "_" + config.MongoColl + "_latestoplog.log"
	} else {
		oplogFile = config.Tspath + "/oplogts/" + config.MongoDB + "_" + config.MongoColl + "_latestoplog.log"
	}

	//init es and mongo
	InitEs(config)
	molog.InfoLog.Printf("connect es success\n")
	cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.MongodbUrl))
	if err != nil {
		molog.ErrorLog.Printf("mongo connect err:%v\n", err)
		return
	}
	defer cli.Disconnect(context.Background())
	molog.InfoLog.Printf("connect mongodb success\n")

	localColl := cli.Database("local").Collection("oplog.rs")
	dbColl := cli.Database(config.MongoDB).Collection(config.MongoColl)

	//开始同步之前找到oplog最新的ts
	var latestoplog OpLog
	filter := validOps()
	opts := &options.FindOneOptions{}
	opts.SetSort(bson.M{"$natural": -1})
	err = localColl.FindOne(context.Background(), filter, opts).Decode(&latestoplog)
	if err != nil {
		molog.ErrorLog.Printf("find latest oplog.rs err:%v\n", err)
		return
	}
	molog.InfoLog.Printf("get latest oplog.rs ts: %v", latestoplog.Timestamp)
	fullSync(cli, config, oplogFile, latestoplog)
	incSync(localColl, dbColl, config, oplogFile, latestoplog)
}

func fullSync(cli *mongo.Client, config *service.Config, oplogFile string, latestoplog OpLog) {
	//扫描要全量同步的mongo数据表，直接批量插入到es
	coll := cli.Database(config.MongoDB).Collection(config.MongoColl)
	find, err := coll.Find(context.Background(), bson.M{})
	if err != nil {
		molog.ErrorLog.Printf("find %s err:%v\n", config.MongoDB+"."+config.MongoColl, err)
		return
	}
	molog.InfoLog.Printf("start sync historical data...")

	mapchan := make(chan map[string]interface{}, 10000)
	syncg := sync.WaitGroup{}

	//三个消费go程，去处理channel发过来的数据
	for i := 0; i < 3; i++ {
		syncg.Add(1)
		go func(i int) {
			molog.InfoLog.Printf("sync historical data goroutine: %d start \n", i)
			defer molog.InfoLog.Printf("sync historical data goroutine: %d exit \n", i)
			defer syncg.Done()
			bulks := make([]esutil.BulkIndexerItem, 5000)
			indexer, _ := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{Client: esClient})
			for {
				count := 0
				for i := 0; i < 5000; i++ {
					select {
					case obj, ok := <-mapchan:
						if !ok {
							break
						}
						id := obj["_id"].(primitive.ObjectID).Hex()
						delete(obj, "_id")
						bytes, err := json.Marshal(obj)
						if err != nil {
							molog.ErrorLog.Printf("sync historical data json marshal err:%v id:%s\n", err, id)
							continue
						}
						doc := esutil.BulkIndexerItem{DocumentID: id, Action: "index", Body: strings.NewReader(string(bytes)), Index: config.MongoDB + "." + config.MongoColl}
						bulks[count] = doc
						count++
					}
				}
				if count != 0 {
					flag := false
					for i := 0; i < count; i++ {
						err := indexer.Add(context.Background(), bulks[i])
						if err != nil {
							flag = true
							molog.ErrorLog.Println("SyncTypeFull error: ", bulks[i], err)
						}
					}
					if flag {
						continue
					}
					indexer.Close(context.Background()) //todo 这种reset方式？
					indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{Client: esClient})
					if err != nil {
						molog.ErrorLog.Println("error: ", err)
					}
					//bulk.Reset()
					count = 0
				} else {
					indexer.Close(context.Background())
					break
				}
			}

		}(i)
	}

	//主go程扫描表，向channel发送给消费的go程
	for {
		ok := find.Next(context.Background())
		if !ok {
			break
		}
		a := make(map[string]interface{})
		err = find.Decode(&a)
		if err != nil {
			molog.ErrorLog.Printf("sync historical data decode %s db err:%v\n", config.MongoDB+"."+config.MongoColl, err)
			break
		}
		mapchan <- a
	}
	close(mapchan)
	syncg.Wait()
	molog.InfoLog.Printf("sync historical data success")

	// 同步完成，将op log 时间戳写入文件（备份，一般来说直接使用latestoplog,用来增量更新即可）
	var op OplogTimestamp
	op.LatestOplogTimestamp = latestoplog.Timestamp
	bytes, err := json.Marshal(op)
	if err != nil {
		molog.ErrorLog.Printf("oplog json marshal err:%v,ts:%v", err, latestoplog)
	}

	f, err := os.Create(oplogFile)
	if err != nil {
		molog.ErrorLog.Printf("os create oplog file err:%v", err)
		return
	}
	_, err = f.Write(bytes)
	if err != nil {
		molog.ErrorLog.Printf("oplog file writer err:%v", err)
		return
	}
	err = f.Close()
	if err != nil {
		return
	}

}

func incSync(localColl, dbColl *mongo.Collection, config *service.Config, oplogFile string, latestoplog OpLog) {
	//根据上面获取的ts，开始重放oplog
	var opCode = [...]string{"c", "i", "u", "d"}
	query := bson.M{
		"ts":          bson.M{"$gte": latestoplog.Timestamp},
		"op":          bson.M{"$in": opCode},
		"ns":          config.MongoDB + "." + config.MongoColl,
		"fromMigrate": bson.M{"$exists": false},
	}
	optss := &options.FindOptions{}
	optss.SetSort(bson.M{"$natural": 1})
	optss.SetCursorType(options.TailableAwait) // 注意这个游标类型

	cursor, err := localColl.Find(context.Background(), query, optss)
	if err != nil {
		molog.ErrorLog.Printf("tail oplog.rs based latestoplog timestamp err:%v\n", err)
		return
	}

	molog.InfoLog.Printf("start sync increment data...")
	insertes := make(chan ElasticObj, 10000)

	// 消费的go程接受channel的数据，放入bulks数组
	go func() {
		indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{Client: esClient})
		if err != nil {
			molog.ErrorLog.Println("error: ", err)
		}
		bulks := make([]esutil.BulkIndexerItem, 0)
		bulksLock := sync.Mutex{}

		//开辟一个定时go程，每秒将bulks数组的数据批量写入es(在es文档中，批量写入是推荐的，除了批量写入吞吐量更快外，还可以优化es查询性能)
		go func() {
			for {
				select {
				case <-time.After(time.Second):
					if len(bulks) == 0 {
						molog.InfoLog.Println("timer len: 0")
						continue
					}
					bulksLock.Lock()
					flag2 := false
					for i := 0; i < len(bulks); i++ {
						err := indexer.Add(context.Background(), bulks[i])
						if err != nil {
							molog.ErrorLog.Println("SyncTypeIncr error: ", bulks[i], err)
							bulksLock.Unlock()
							flag2 = true
							break
						}
					}
					if flag2 {
						continue
					}

					indexer.Close(context.Background())
					indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{Client: esClient})
					if err != nil {
						molog.ErrorLog.Println("error: ", err)
					}
					molog.InfoLog.Println("timer will reset:", len(bulks))
					bulks = make([]esutil.BulkIndexerItem, 0)
					bulksLock.Unlock()
				}
			}
		}()

		for {
			select {
			case obj := <-insertes:
				bytes, err := json.Marshal(obj)
				if err != nil {
					molog.ErrorLog.Printf("data json marshal err:%v id:%s\n", err, obj.ID)
					continue
				}
				molog.InfoLog.Println("oplog receive:", obj.ID)
				doc := esutil.BulkIndexerItem{DocumentID: obj.ID, Action: "index", Body: strings.NewReader(string(bytes)), Index: config.MongoDB + "." + config.MongoColl}

				bulksLock.Lock()
				bulks = append(bulks, doc)
				bulksLock.Unlock()
			}
		}

	}()

	var ts OplogTimestamp
	//每个小时纪录一次最新的oplog
	go func() {
		for {
			time.Sleep(time.Second * 60 * 60)
			if ts.LatestOplogTimestamp.T > latestoplog.Timestamp.T {
				f, err := os.Create(oplogFile)
				if err != nil {
					molog.ErrorLog.Printf("os create oplog file err:%v", err)
					return
				}
				bytes, err := json.Marshal(ts)
				if err != nil {
					molog.ErrorLog.Printf("oplog json marshal err:%v,ts:%v", err, ts)
				}
				_, err = f.Write(bytes)
				if err != nil {
					molog.ErrorLog.Printf("oplog file writer err:%v", err)
					return
				}
				err = f.Close()
				if err != nil {
					return
				}
			}
		}
	}()

	// 主go程遍历oplog表
	for cursor.Next(context.Background()) {
		o := OpLog{}
		err = cursor.Decode(&o)
		if err != nil {
			molog.ErrorLog.Printf("tail decode oplog.rs err:%v\n", err)
			continue
		}
		ts.LatestOplogTimestamp = o.Timestamp
		switch o.Operation {
		case "i":
			id := o.Doc["_id"].(primitive.ObjectID).Hex()
			delete(o.Doc, "_id")
			var obj ElasticObj
			obj.ID = id
			obj.Obj = o.Doc
			molog.InfoLog.Println("oplog i:", id)
			insertes <- obj
		case "d":
			id := o.Doc["_id"].(primitive.ObjectID).Hex()
			//_, err := esCli.Delete().Index(config.MongoDB + "." + config.MongoColl).Type("_doc").Id(id).Do(context.Background())
			req := esapi.DeleteRequest{Index: config.MongoDB + "." + config.MongoColl, DocumentID: id}
			_, err := req.Do(context.Background(), GetEs())
			if err != nil {
				molog.ErrorLog.Printf("delete document in es err:%v id:%s\n", err, id)
				continue
			}
			molog.InfoLog.Println("oplog d:", id)
		case "u":
			id := o.Update["_id"].(primitive.ObjectID).Hex()
			objId, err := primitive.ObjectIDFromHex(id)
			if err != nil {
				molog.ErrorLog.Printf("objectid id err:%v id:%s\n", err, id)
				continue
			}
			f := bson.M{
				"_id": objId,
			}
			obj := make(map[string]interface{})
			err = dbColl.FindOne(context.Background(), f).Decode(&obj)
			if err != nil {
				molog.ErrorLog.Printf("find document from mongodb  err:%v id:%s\n", err, id)
				continue
			}
			delete(obj, "_id")
			var elasticObj ElasticObj
			elasticObj.ID = id
			elasticObj.Obj = obj
			molog.InfoLog.Println("oplog u:", id)
			insertes <- elasticObj
		}

	}
}
