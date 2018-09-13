package com.core.service;

import java.util.Map;
import java.util.Set;
import java.util.List;
import org.slf4j.Logger;
import java.util.Iterator;
import java.util.ArrayList;
import com.core.bean.PageBean;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import java.net.UnknownHostException;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Service;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.springframework.beans.factory.annotation.Autowired;





/**
 * Elastic Search 服务
 * 
 * @author zsh10649
 *
 */
@Service
public class ElasticSearchServiceImpl {

	// 日志
	Logger logger = LoggerFactory.getLogger(ElasticSearchServiceImpl.class);



	// 字段
	@Autowired
	private TransportClient transportClient;

	@Value("${spring.elasticSearch.timeOutMillis}")
	private String timeOutMillis;



	// 方法
	/**
	 * 分页查询数据
	 * 
	 * @param
	 * termQueryParams ( field : value )
	 * matchQueryParams ( field : value )
	 * multiMatchQueryParams ( value : field set )
	 * 
	 */
	public <T> PageBean<T> pageSearch(String index, String type, Map<String, Object> termQueryParams, List<Map<String, Object>> matchQueryList,
			Map<String, Set<String>> multiMatchQueryParams, Map<String, ArrayList<String>> rangeQueryParams, List<BoolQueryBuilder> boolQueryBuilderList, 
			Set<String> sortFields, int pageNo, int pageSize, Class<T> clazz) throws UnknownHostException {

		// 创建查询构建者
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		// 创建查询
		// 1) TermQueryBuilder
		if(termQueryParams != null && termQueryParams.size() > 0) {
			Set<String> keySet = termQueryParams.keySet();

			for(String key : keySet) {
				TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(key, termQueryParams.get(key));
				boolQueryBuilder.must(termQueryBuilder);
			}
		}

		// 2) MatchQueryBuilder
		if (matchQueryList != null && matchQueryList.size() > 0) {
			for(Map<String, Object> matchQuery : matchQueryList) {
				Set<String> keySet = matchQuery.keySet();

				for(String key : keySet) {
					MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(key, matchQuery.get(key));
					boolQueryBuilder.must(matchQueryBuilder);
				}
			}
		}

		// 3) MultiMatchQueryBuilder
		if (multiMatchQueryParams != null && multiMatchQueryParams.size() > 0) {
			Set<String> keySet = multiMatchQueryParams.keySet();

			for(String key : keySet) {
				Set<String> fieldSet = multiMatchQueryParams.get(key);
				MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(key, fieldSet.toArray(new String[fieldSet.size()]));

				boolQueryBuilder.must(multiMatchQueryBuilder);
			}
		}

		// 4) RangeQueryBuilder
		if (rangeQueryParams != null && rangeQueryParams.size() > 0) {
			Set<String> keySet = rangeQueryParams.keySet();

			for(String key : keySet) {
				ArrayList<String> rangeValueList = rangeQueryParams.get(key);

				RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(key);
				rangeQueryBuilder.from(rangeValueList.get(0), Boolean.TRUE);
				rangeQueryBuilder.to(rangeValueList.get(1), Boolean.TRUE);

				boolQueryBuilder.must(rangeQueryBuilder);
			}
		}

		// 5) BoolQueryBuilderList
		if (boolQueryBuilderList != null && boolQueryBuilderList.size() > 0) {
			for(BoolQueryBuilder queryBuilder : boolQueryBuilderList) {
				boolQueryBuilder.must(queryBuilder);
			}
		}

		// 创建查询请求
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);

        searchRequestBuilder.setTypes(type);
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 排序字段
        if (sortFields != null && sortFields.size() > 0) {
        	Iterator<String> iterator = sortFields.iterator();

        	while (iterator.hasNext()) {
        		searchRequestBuilder.addSort(iterator.next(), SortOrder.DESC);
        	}
        }

        searchRequestBuilder.setQuery(boolQueryBuilder);

        // 分页应用
        pageNo = ( pageNo == 0 ? 1 : pageNo );
        int pageFromIndex = ( pageNo - 1 ) * pageSize;
        searchRequestBuilder.setFrom(pageFromIndex).setSize(pageSize);

        // 执行搜索，返回响应
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet(Long.valueOf(timeOutMillis));

        // 处理响应
        long totalHits = searchResponse.getHits().getTotalHits();
        long length = searchResponse.getHits().getHits().length;

        // 创建分页结果
        PageBean<T> pagingResult = new PageBean<T>();

        if (searchResponse.status().getStatus() == 200) {
            List<T> sourceList = new ArrayList<T>();

            for(SearchHit searchHit : searchResponse.getHits().getHits()) {
                T tInstance = JSON.parseObject(JSON.toJSONString(searchHit.getSourceAsMap()), clazz);
                sourceList.add(tInstance);
            }

            pagingResult.setCurrentPage(pageNo);
            pagingResult.setPageCount( (totalHits % pageSize) == 0 ? Integer.valueOf(String.valueOf(totalHits / pageSize)) : Integer.valueOf(String.valueOf((totalHits / pageSize) + 1)) );
            pagingResult.setPageSize(pageSize);
            pagingResult.setRecordList(sourceList);
            pagingResult.setRecordCount(Integer.valueOf(String.valueOf(totalHits)));
        }

        return pagingResult;
	}



	



}