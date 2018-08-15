package com.core.controller;

import java.util.Map;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.net.UnknownHostException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.core.bean.PageBean;
import com.core.service.ElasticSearchServiceImpl;





@RestController
public class Controller {
	
	

	//1. 字段
	@Autowired
	private TransportClient transportClient;

	@Autowired
	private ElasticSearchServiceImpl elasticSearchServiceImpl;





	/**
	 * 创建 空索引
	 * 
	 * indices 就是索引
	 * 
	 */
	@GetMapping("/createIndex")
	@ResponseBody
	public boolean createIndex(@RequestParam(name = "index") String index) throws IOException {
		// 1. 创建客户端
		AdminClient adminClient = transportClient.admin();
		IndicesAdminClient indicesAdminClient = adminClient.indices();

		// 2. 创建请求
		IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(index);

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
			CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate(index).execute().actionGet(60 * 1000);
			return createIndexResponse.isAcknowledged();
		}
	}

	
	
	
	
	/**
	 * 为空索引添加映射
	 * 
	 * 设置映射API允许我们在指定索引上一次性创建或修改一到多个索引的映射。设置映射必须确保指定的索引存在，否则会报错。
	 * 
	 */
	@GetMapping("/putMapping")
	@ResponseBody
	public boolean putMapping(@RequestParam(name = "index") String index, @RequestParam(name = "type") String type) throws Exception {
		// mapping
		XContentBuilder mappingBuilder;

		try {
			mappingBuilder = XContentFactory.jsonBuilder();

			mappingBuilder.startObject();
			mappingBuilder.startObject(type);
			mappingBuilder.startObject("properties");

			// t_fashion_information
			/*mappingBuilder.startObject("id").field("type", "keyword").field("store", Boolean.TRUE).endObject()
						    .startObject("res_no").field("type", "text").field("store", Boolean.TRUE).endObject()

						    .startObject("title").field("type", "text")
						    .field("analyzer", "ik_max_word")
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
						    .startObject("batch_no").field("type", "text").field("store", Boolean.TRUE).endObject();*/

			// t_shop
			/* mappingBuilder.startObject("id").field("type", "keyword").field("store", Boolean.TRUE).endObject()
			.startObject("out_no").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("shop_name").field("type", "text")
			.field("analyzer", "ik_max_word")
			.field("search_analyzer", "ik_max_word")
			.field("store", Boolean.TRUE)
			.endObject()
			.startObject("shop_address").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("shop_logo").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("index_url").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("status").field("type", "long").field("store", Boolean.TRUE).endObject()
			.startObject("create_user").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("create_time").field("type", "date").field("store", Boolean.TRUE).endObject()
			.startObject("modify_user").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("modify_time").field("type", "date").field("store", Boolean.TRUE).endObject()
			.startObject("batch_no").field("type", "text").field("store", Boolean.TRUE).endObject(); */

			// selection
			mappingBuilder.startObject("id").field("type", "keyword").field("store", Boolean.TRUE).endObject()
			.startObject("res_no").field("type", "text").field("store", Boolean.TRUE).endObject()

			.startObject("title").field("type", "text")
			.field("analyzer", "ik_max_word")
			.field("search_analyzer", "ik_max_word")
			.field("store", Boolean.TRUE)
			.endObject()

			.startObject("branch").field("type", "keyword").field("store", Boolean.TRUE).endObject()
			.startObject("branch_analyze").field("type", "text")
			.field("analyzer", "ik_max_word")
			.field("search_analyzer", "ik_max_word")
			.field("store", Boolean.TRUE)
			.endObject()

			.startObject("to_new_date").field("type", "date").field("store", Boolean.TRUE).endObject()
			.startObject("shop_id").field("type", "keyword").field("store", Boolean.TRUE).endObject()
			.startObject("price").field("type", "double").field("store", Boolean.TRUE).endObject()
			.startObject("original_price").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("res_url").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("outer_res_no").field("type", "text").field("store", Boolean.TRUE).endObject()

			.startObject("res_attr").field("type", "text")
			.field("analyzer", "ik_max_word")
			.field("search_analyzer", "ik_max_word")
			.field("store", Boolean.TRUE)
			.endObject()

			.startObject("status").field("type", "keyword").field("store", Boolean.TRUE).endObject()
			.startObject("create_time").field("type", "date").field("store", Boolean.TRUE).endObject()
			.startObject("modify_time").field("type", "date").field("store", Boolean.TRUE).endObject()
			.startObject("m_sale_count").field("type", "long").field("store", Boolean.TRUE).endObject()
			.startObject("accum_comment_count").field("type", "long").field("store", Boolean.TRUE).endObject()
			.startObject("popularity_count").field("type", "long").field("store", Boolean.TRUE).endObject()
			.startObject("sale_count").field("type", "long").field("store", Boolean.TRUE).endObject()

			.startObject("shop_name").field("type", "keyword").field("store", Boolean.TRUE).endObject()
			.startObject("shop_name_analyze").field("type", "text")
			.field("analyzer", "ik_max_word")
			.field("search_analyzer", "ik_max_word")
			.field("store", Boolean.TRUE)
			.endObject()

			.startObject("shop_address").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("shop_logo").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("index_url").field("type", "text").field("store", Boolean.TRUE).endObject()
			.startObject("source_media").field("type", "keyword").field("store", Boolean.TRUE).endObject();

			mappingBuilder.endObject();
			mappingBuilder.endObject();
			mappingBuilder.endObject();
		} catch (Exception e) {
			System.out.println("--------- createIndex 创建 mapping 失败：");
			return false;
		}

		AdminClient adminClient = transportClient.admin();
		IndicesAdminClient indicesAdminClient = adminClient.indices();
		PutMappingResponse response = indicesAdminClient.preparePutMapping(index).setType(type).setSource(mappingBuilder).execute().actionGet(60 * 1000);

		return response.isAcknowledged();
	}





	/**
	 * 删除索引
	 * 
	 * @param
	 * index: 索引名称
	 * 
	 */
	@GetMapping(value = "/removeIndex")
	@ResponseBody
	public boolean removeIndex(@RequestParam(name = "index") String index) throws UnknownHostException {
		// 1. 创建客户端
		AdminClient adminClient = transportClient.admin();
		IndicesAdminClient indicesAdminClient = adminClient.indices();
		IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(index);

		ActionFuture<IndicesExistsResponse> actionFutureExists = indicesAdminClient.exists(indicesExistsRequest);
		IndicesExistsResponse indicesExistsResponse = actionFutureExists.actionGet();

		if(indicesExistsResponse.isExists()) {
			System.out.println("索引存在，能够删除...");
			DeleteIndexResponse response = indicesAdminClient.prepareDelete(index).execute().actionGet(60 * 1000);
			return response.isAcknowledged();
		} else {
			System.out.println("索引不存在，无法删除...");
			return Boolean.FALSE;
		}
	}






	/**
	 * 
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
	 * @throws UnknownHostException 
	 * 
	 */
	@GetMapping("/getById")
	@ResponseBody
	public ResponseEntity getById(@RequestParam(name = "index") String index, @RequestParam(name = "type") String type, @RequestParam(name = "id") String id) throws UnknownHostException {
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
	 * @throws UnknownHostException 
	 * 
	 */
	@GetMapping("/pageSearch")
	@ResponseBody
	public PageBean pageSearch(String field, String value, String sortField, String highlightField, int pageNo, int pageSize) throws UnknownHostException {
		// 执行查询
		SearchResponse searchResponse = queryElasticSearch("index1", "t_fashion_information", field, value, sortField, highlightField, pageNo, pageSize);

        // 
        long totalHits1 = searchResponse.getHits().getTotalHits();
        long length1 = searchResponse.getHits().getHits().length;

        System.out.println("index1 共查询到 " + totalHits1 + " 条数据，处理数据条数" + length1);

        //
        List<Map<String, Object>> listDataAll = new ArrayList<Map<String, Object>>();

        if (searchResponse.status().getStatus() == 200) {
        	// 解析对象
            List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, null /*highlightField*/);
            listDataAll.addAll(sourceList);
        }

        return new PageBean(pageNo, pageSize, (int) ( totalHits1 ), listDataAll);
    }




        
    /**
	 * 分页查询数据
	 * 
	 * @url:
	 * http://localhost:8080/pageSearch2?index=index3&type=selection&shopName=裂帛服饰旗舰店&branch=裂帛&sourceMedia=4&value1=旗舰店&title1=连衣裙&title2=新款&title3=收腰
	 * 
	 * 对于termQuery进行完全匹配的字段，需要把字段类型设置为keyword
	 * 
	 * @param
	 * 
	 */
	@GetMapping("/pageSearch2")
	@ResponseBody
	public PageBean pageSearch2(@RequestParam(name = "index") String index, @RequestParam(name = "type") String type, 
			@RequestParam(name = "shopName") String shopName, @RequestParam(name = "branch") String branch, @RequestParam(name = "sourceMedia") String sourceMedia, @RequestParam(name = "value1") String value1,
			@RequestParam(name = "title1") String title1, @RequestParam(name = "title2") String title2, @RequestParam(name = "title3") String title3) throws UnknownHostException {

		int startPage = 1;
		int pageSize = 10;
		String[] fields = {"title"};
		String sortField = null;

		// 创建查询
		TermQueryBuilder termQueryBuilder_1 = QueryBuilders.termQuery("shop_name", shopName);
		TermQueryBuilder termQueryBuilder_2 = QueryBuilders.termQuery("branch", branch);
		TermQueryBuilder termQueryBuilder_3 = QueryBuilders.termQuery("source_media", sourceMedia);
		MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(value1, "title", "shop_name_analyze", "branch_analyze", "res_attr");
		MatchQueryBuilder matchQueryBuilder1 = QueryBuilders.matchQuery("title", title1);
		MatchQueryBuilder matchQueryBuilder2 = QueryBuilders.matchQuery("title", title2);
		MatchQueryBuilder matchQueryBuilder3 = QueryBuilders.matchQuery("title", title3);

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		boolQueryBuilder
		.must(termQueryBuilder_1)
		.must(termQueryBuilder_2)
		.must(termQueryBuilder_3)
		.must(multiMatchQueryBuilder)
		.must(matchQueryBuilder1)
		.must(matchQueryBuilder2)
		.must(matchQueryBuilder3);

		// 创建查询请求
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);

        searchRequestBuilder.setTypes(type);
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        //
        //searchRequestBuilder.setFetchSource(fields, null);

        // 排序字段
        if (sortField != null) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        //searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setQuery(boolQueryBuilder);

        // 分页应用
        searchRequestBuilder.setFrom(startPage).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);

        // 执行搜索,返回搜索响应信息
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet(60 * 1000);

        long totalHits = searchResponse.getHits().getTotalHits();
        long length = searchResponse.getHits().getHits().length;

        System.out.println("共查询到" + totalHits + "条数据，处理数据条数" + length);
        
        if (searchResponse.status().getStatus() == 200) {
        	// 解析对象
            List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();

            //
            for(SearchHit searchHit : searchResponse.getHits().getHits()) {
                sourceList.add(searchHit.getSourceAsMap());
            }

            //pagingMapResult.setTotalCount(totalHits);
            //pagingMapResult.setPageNo(startPage);
            //pagingMapResult.setPageSize(pageSize);
            //pagingMapResult.setMapResult(sourceList);
            
            return new PageBean(startPage, pageSize, 0, sourceList);
        } else {
        	return null;
        }
    }
    
    
    
    
	
	/**
     * 
     * 按条件执行查询
     * 
     */
    private SearchResponse queryElasticSearch(String index, String type, String field, String value, String sortField, String highlightField, int pageNo, int pageSize) {
    	// 创建查询
		// 1) 布尔查询
    	BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		// 2) 拆字查询
    	QueryBuilder query = QueryBuilders.matchQuery(field, value);
    	
    	// 3) 拆字多匹配查询
    	MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(value, field);

    	// 4) Term 精确匹配
    	TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(field, value);

    	// 5) 匹配所有字段
    	MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

    	// 设置 查询时候 分词器 queryBuilder.analyzer("ik_smart");

		// 创建查询请求
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        searchRequestBuilder.setTypes(type);
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
        searchRequestBuilder.setFetchSource(field, null);

        // 排序字段
        // 1）先按 相似度 排序
        searchRequestBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));

        // 2）再按 字段 排序
        if (sortField != null) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        // 高亮（xxx=111, aaa=222）
        if (highlightField != null) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            highlightBuilder.preTags("<span style='color:red' >");     //设置前缀
            highlightBuilder.postTags("</span>");                      //设置后缀

            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        //
        searchRequestBuilder.setQuery(query);

        // 分页应用
        searchRequestBuilder.setFrom((pageNo - 1) * pageSize).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(Boolean.TRUE);

        // 执行搜索, 返回搜索响应信息
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet(60 * 1000);

        // 返回查询结果
        return searchResponse;
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



}