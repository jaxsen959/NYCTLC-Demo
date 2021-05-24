package com.nwcd.bigdata

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

object EsUtil {
  var factory:JestClientFactory = null;

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())

  }

  // 增加一个文档（row）
  def addDoc(source:Any, indexName:String): Unit = {
    val jest: JestClient = getClient
    //build设计模式
    //内容是可转化为json对象  HashMap
    val index = new Index.Builder(source).index(indexName).`type`("_doc").id("0104").build()
    val message: String = jest.execute(index).getErrorMessage
    if (message!=null){
      println(message)
    }
    jest.close()
  }

  // 批操作增加文档
  def bulkDoc(sourceList:List[Any],indexName:String): Unit = {
    if(sourceList!=null && sourceList.size>0){
      val jest: JestClient = getClient
      //build设计模式
      val bulkBuilder = new Bulk.Builder
      for (source <- sourceList){
        val index = new Index.Builder(source).index(indexName).`type`("_doc").build()
        bulkBuilder.addAction(index)
      }
      val bulk:Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)

//      val items: util.List[BulkResult#BulkResultItem]
      result.getItems.size()
      println("保存到ES:"+result.getItems.size()+"条数")
      jest.close()
    }
  }

  case class Movie0105(id:String, movie_name:String, name:String)
}
