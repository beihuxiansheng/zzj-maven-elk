package com.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.action.ActionFuture;
import org.springframework.http.ResponseEntity;
import org.elasticsearch.action.get.GetResponse;
import org.springframework.boot.SpringApplication;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.core.bean.PageBean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;





/**
 * Spring boot 整合 elastic search
 *
 */
@SpringBootApplication
@RestController
public class App {

	//1. 字段
	@Autowired
	private TransportClient transportClient;



	/**
	 * 创建 空索引
	 * 
	 */
	@GetMapping("/createIndex")
	@ResponseBody
	public boolean createIndex(@RequestParam(name = "indexName", defaultValue = "") String indexName) throws IOException {
		AdminClient adminClient = transportClient.admin();
		IndicesAdminClient indicesAdminClient = adminClient.indices();

		IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(indexName);

		ActionFuture<IndicesExistsResponse> actionFutureExists = indicesAdminClient.exists(indicesExistsRequest);
		IndicesExistsResponse indicesExistsResponse = actionFutureExists.actionGet();

		if(indicesExistsResponse.isExists()) {
			System.out.println("索引已经存在...");
			return Boolean.FALSE;
		} else {
			System.out.println("索引不存在...");

			//CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate(indexName);
			//ActionFuture<CreateIndexResponse> actionFutureCreate = createIndexRequestBuilder.execute();
			//CreateIndexResponse createIndexResponse = actionFutureCreate.actionGet();
			CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate(indexName).execute().actionGet();

			return createIndexResponse.isAcknowledged();
		}
	}

	
	
	
	
	/**
	 * 为空索引添加映射
	 * 
	 * 设置映射API允许我们在指定索引上一次性创建或修改一到多个索引的映射。设置映射必须确保指定的索引存在，否则会报错。
	 * 
	 */
	@GetMapping("/putIndexMapping")
	@ResponseBody
	public boolean putIndexMapping(@RequestParam(name = "index") String index, @RequestParam(name = "type") String type) throws Exception {
		// mapping
		XContentBuilder mappingBuilder;

		try {
			mappingBuilder = XContentFactory.jsonBuilder();

			mappingBuilder.startObject();
			mappingBuilder.startObject(type);
			mappingBuilder.startObject("properties");

			mappingBuilder.startObject("id").field("type", "keyword").field("store", Boolean.TRUE).endObject()
						  .startObject("res_no").field("type", "text").field("store", Boolean.TRUE).endObject()

						  .startObject("title").field("type", "text")
						  .field("index_analyzer", "ik_max_word")
						  .field("search_analyzer", "ik_max_word")
						  .field("store", Boolean.TRUE)
						  .endObject()

						  .startObject("cover_pic_id").field("type", "long").field("store", Boolean.TRUE).endObject()
						  .startObject("source_id").field("type", "long").field("store", Boolean.TRUE).endObject()
						  .startObject("res_attr").field("type", "text").field("store", Boolean.TRUE).field("fielddata", Boolean.TRUE).endObject()
						  .startObject("outer_res_no").field("type", "text").field("store", Boolean.TRUE).endObject()
						  .startObject("status").field("type", "short").field("store", Boolean.TRUE).endObject()
						  .startObject("praise_count").field("type", "short").field("store", Boolean.TRUE).endObject()
						  .startObject("forward_count").field("type", "short").field("store", Boolean.TRUE).endObject()
						  .startObject("comment_count").field("type", "short").field("store", Boolean.TRUE).endObject()
						  .startObject("release_time").field("type", "date").field("store", Boolean.TRUE).endObject()
						  .startObject("create_time").field("type", "date").field("store", Boolean.TRUE).endObject()
						  .startObject("modify_time").field("type", "date").field("store", Boolean.TRUE).endObject()
						  .startObject("batch_no").field("type", "text").field("store", Boolean.TRUE).endObject();

			mappingBuilder.endObject();
			mappingBuilder.endObject();
			mappingBuilder.endObject();

		} catch (Exception e) {
			System.out.println("--------- createIndex 创建 mapping 失败：");
			return false;
		}

		AdminClient adminClient = transportClient.admin();
		IndicesAdminClient indicesAdminClient = adminClient.indices();

		PutMappingResponse response = indicesAdminClient.preparePutMapping(index).setType(type).setSource(mappingBuilder).get();

		return response.isAcknowledged();
	}





	/**
	 * 删除索引
	 * 
	 */
	@GetMapping(value = "/deleteIndex")
	@ResponseBody
	public boolean deleteIndex(@RequestParam(name = "index") String index) {
		IndicesAdminClient indicesAdminClient = transportClient.admin().indices();
		DeleteIndexResponse response = indicesAdminClient.prepareDelete(index).execute().actionGet();
		return response.isAcknowledged();
	}
	
	
	



	/**
	 * 创建复杂索引
	 * 
	 * 下面代码创建复杂索引，给它设置它的映射(mapping)和设置信息(settings)，指定分片个数为3，副本个数为2，同时设置school字段不分词。
	 * 
	 */
	/*public static boolean createIndex(Client client, String index) {
		// settings
		Settings settings = Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2).build();

		// mapping
		XContentBuilder mappingBuilder;

		try {
			mappingBuilder = XContentFactory.jsonBuilder()
							.startObject()
							.startObject(index)
							.startObject("properties")
							.startObject("name").field("type", "string").field("store", true).endObject()
							.startObject("sex").field("type", "string").field("store", true).endObject()
							.startObject("college").field("type", "string").field("store", true).endObject()
							.startObject("age").field("type", "integer").field("store", true).endObject()
							.startObject("school").field("type", "string").field("store", true).field("index", "not_analyzed").endObject()
							.endObject()
							.endObject()
							.endObject();
		} catch (Exception e) {
			logger.error("--------- createIndex 创建 mapping 失败：",e);
			return false;
		}

		IndicesAdminClient indicesAdminClient = client.admin().indices();
		CreateIndexResponse response = indicesAdminClient.prepareCreate(index)
		.setSettings(settings)
		.addMapping(index, mappingBuilder)
		.get();
		return response.isAcknowledged();

	}*/










	/**
	 * 根据ID查询数据
	 * 
	 */
	@GetMapping("/getDataById")
	@ResponseBody
	public ResponseEntity getData(@RequestParam(name = "index") String index, @RequestParam(name = "type") String type, @RequestParam(name = "id") String id) {
		GetRequestBuilder getRequestBuilder = transportClient.prepareGet(index, type, id);
		GetResponse getResponse = getRequestBuilder.get();

		if(!getResponse.isExists()) {
			return new ResponseEntity(getResponse, HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity(getResponse, HttpStatus.OK);
	}







	/**
	 * 分页查询数据
	 * 
	 */
	@GetMapping("/searchDataPage")
	@ResponseBody
	public PageBean searchDataPage(@RequestParam(name = "index") String index, @RequestParam(name = "type") String type) {
		
		int startPage = 1;
		int pageSize = 10;
		String[] fields = {"title"};
		String sortField = null;
		String highlightField = null;

		//
		QueryBuilder query = QueryBuilders.matchQuery("title", "球星拍成");

		// 创建查询请求
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);

        searchRequestBuilder.setTypes(type);
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
        /*if (fields != null && fields.contains(",")) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }*/
        searchRequestBuilder.setFetchSource(fields, null);

        // 排序字段
        if (sortField != null) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        // 高亮（xxx=111,aaa=222）
        if (highlightField != null) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            //highlightBuilder.preTags("<span style='color:red' >");     //设置前缀
            //highlightBuilder.postTags("</span>");                      //设置后缀

            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        //searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setQuery(query);

        // 分页应用
        searchRequestBuilder.setFrom(startPage).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);

        // 执行搜索,返回搜索响应信息
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        System.out.println("共查询到" + totalHits + "条数据,处理数据条数" + length);

        if (searchResponse.status().getStatus() == 200) {
        	// 解析对象
            List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

            return new PageBean(startPage, pageSize, (int) totalHits, sourceList);
        } else {
        	return null;
        }

    }





	/**
     * 高亮结果集 特殊处理
     *
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        StringBuffer stringBuffer = new StringBuffer();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (highlightField != null) {
                System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();
                if (text != null) {
                	for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                	//遍历 高亮结果集，覆盖 正常结果集
                    searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }

        return sourceList;
    }
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	/**
	 * 
	 */
    public static void main( String[] args ) {
    	SpringApplication.run(App.class, args);
        System.out.println( "Spring boot integrate elastic search starts!" );
    }

    
    
    
    
}