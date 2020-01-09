package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.atguigu.pojo.User;
import com.atguigu.repository.UserRepository;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class EsDemoTest {

    @Autowired
    private ElasticsearchRestTemplate restTemplate;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void test(){
        restTemplate.createIndex(User.class);
        restTemplate.putMapping(User.class);
    }

    @Test
    public void testDocument(){
        userRepository.save(new User(1L,"张三",23,"123456"));
    }
    @Test
    public void testSaveAll(){
        List<User> users = new ArrayList<>();
        users.add(new User(1l, "柳岩", 18, "123456"));
        users.add(new User(2l, "范冰冰", 19, "123456"));
        users.add(new User(3l, "李冰冰", 20, "123456"));
        users.add(new User(4l, "锋哥", 21, "123456"));
        users.add(new User(5l, "小鹿", 22, "654321"));
        users.add(new User(6l, "韩红", 23, "654321"));
        users.add(new User(6l, "韩冰冰", 23, "654321"));
        userRepository.saveAll(users);
    }

    @Test
    public void testDelete(){
        userRepository.deleteById(6L);
    }

    /**
     * 方式一 （获取高亮较难，如果做高亮，需要另外两种方式）
     * repository的玩法
     * 基本查询
     * 步骤
     * （1）使用接口继承 ElasticsearchRepository<T, ID>
     * （2）注入继承接口的实例
     * （3）使用上面实例调取方法 ，可以在接口中自定义方法，但要遵循命名规范
     * 包括自定义查询
     */
    @Test
    public void testQuery(){
        //System.out.println(userRepository.findById(1L));
        //userRepository.findAllById(Arrays.asList(1L,2L,3L)).forEach(System.out::println);
        //userRepository.findByAgeBetween(19,21).forEach(System.out::println);
        userRepository.findByNative(19,21).forEach(System.out::println);
    }

    /**
     * Repository的search增强查询，
     * search()方法常用的重载
     * 
     *   Iterable<T> search(QueryBuilder var1);
     *   Page<T> search(QueryBuilder var1, Pageable var2); ,但不能进行聚合查询（缺点）,还是不够强大
     *
     *   Page<T> search(SearchQuery var1);是对上面方法的增强，需要使用new NativeSearchQueryBuilder();//SearchQuery的工具类
     *   用工具类进行扩展高亮，排序，聚合的操作
     * 配合使用QueryBuilders工具类提供的快速查询方法
     * PageRequest.of(1, 2)查询分页    页数从第0页开始
     */
    @Test
    public void testSearch(){
        //范围查询
        //userRepository.search(QueryBuilders.rangeQuery("age").gte(19).lte(22)).forEach(System.out::println);
        //分页查询
//        Page<User> page = userRepository.search(QueryBuilders.rangeQuery("age").gte(19).lte(22)
//                , PageRequest.of(1, 2));
//        System.out.println("总记录数："+page.getTotalElements());
//        System.out.println("总页数："+page.getTotalPages());
//        page.getContent().forEach(System.out::println);


        //初始化自定义查询的构建器【高亮，分页】
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();//SearchQuery的工具类

        //匹配查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        //排序 ,使用 SortBuilders工具类
        queryBuilder.withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC));
        //分页 ,使用PageRequest的静态方法of
        queryBuilder.withPageable(PageRequest.of(0,2));
        //高亮查询  但获取高亮结果集较难
        queryBuilder.withHighlightBuilder(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        //聚合查询 使用AggregationBuilders工具类
        queryBuilder.addAggregation(AggregationBuilders.terms("passwordAgg").field("password"));

        AggregatedPage<User> page = (AggregatedPage)userRepository.search(queryBuilder.build());
        System.out.println("总记录数："+page.getTotalElements());
        System.out.println("总页数："+page.getTotalPages());
        page.getContent().forEach(System.out::println);

        //获取指定聚合名称的聚合
        ParsedStringTerms terms = (ParsedStringTerms) page.getAggregation("passwordAgg");
        //获取聚合的桶，两种密码就有两种桶
        terms.getBuckets().forEach(bucket -> {
            System.out.println(bucket.getKeyAsString());
        });
    }

    /**
     * 方式二  (更加底层一点，灵活，但需要json的序列化与反序列化)
     * restTemplate的玩法，获得的是json串，需要反序列化获取对象
     */
    @Test
    public void testSearch2(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        //匹配查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        //排序 ,使用 SortBuilders工具类
        queryBuilder.withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC));
        //分页 ,使用PageRequest的静态方法of
        queryBuilder.withPageable(PageRequest.of(0,2));
        //高亮查询  但获取高亮结果集较难
        queryBuilder.withHighlightBuilder(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        //聚合查询 使用AggregationBuilders工具类
        queryBuilder.addAggregation(AggregationBuilders.terms("passwordAgg").field("password"));


        restTemplate.query(queryBuilder.build(),response->{
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                String userJson = hit.getSourceAsString();
                User user = JSON.parseObject(userJson, User.class);
                System.out.println(user);
                //System.out.println(userJson);
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("name");

                user.setName(highlightField.getFragments()[0].string());
                System.out.println(user);
            }

            //聚合操作
            Map<String, Aggregation> asMap = response.getAggregations().asMap();
            ParsedStringTerms terms = (ParsedStringTerms) asMap.get("passwordAgg");
            terms.getBuckets().forEach(bucket->{
                System.out.println(bucket.getKeyAsString());
            });
            return null;
        });
    }

    /**
     * 方式三（方式二与方式三功能相同，推荐方式三（原生，不依赖框架））
     * elasticsearch原生客户端
     */
    @Test
    public void testHighLevelClient() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //匹配查询
        sourceBuilder.query(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        //排序
        sourceBuilder.sort("age",SortOrder.DESC);
        //分页
        sourceBuilder.from(0);
        sourceBuilder.size(2);

        //高亮
        sourceBuilder.highlighter(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));

        //聚合
        sourceBuilder.aggregation(AggregationBuilders.terms("passwordAgg").field("password").
                subAggregation(AggregationBuilders.avg("ageAgg").field("age")));

        SearchResponse response = restHighLevelClient.search(new SearchRequest(new String[]{"user"},sourceBuilder), RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            String userJson = hit.getSourceAsString();
            User user = JSON.parseObject(userJson, User.class);
            System.out.println(user);
//                System.out.println(userJson);
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("name");
            //System.out.println(highlightField.getFragments()[0].string());
            user.setName(highlightField.getFragments()[0].string());
            System.out.println(user);
        }

        Map<String, Aggregation> asMap = response.getAggregations().getAsMap();
        ParsedStringTerms passwordAgg = (ParsedStringTerms)asMap.get("passwordAgg");
        passwordAgg.getBuckets().forEach(bucket ->{
            System.out.println(bucket.getKeyAsString());
            Map<String, Aggregation> ageMap = bucket.getAggregations().getAsMap();
            ParsedAvg terms = (ParsedAvg) ageMap.get("ageAgg");
            System.out.println(terms.getValueAsString());
        });

    }

}
