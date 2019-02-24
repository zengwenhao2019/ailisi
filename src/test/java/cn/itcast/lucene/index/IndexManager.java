package cn.itcast.lucene.index;

import cn.itcast.lucene.dao.impl.BookImpl;
import cn.itcast.lucene.pojo.Book;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;
import org.wltea.analyzer.lucene.IKAnalyzer;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class IndexManager {
    @Test
    public void createIndex() throws Exception{
        //采集数据
        BookImpl bookDao = new BookImpl();
        List<Book> bookList = bookDao.findAll();
        //创建文档集合
        ArrayList<Document> documents = new ArrayList<>();
        for (Book book : bookList) {
            //创建文档对象
            Document doc = new Document();
            //给文档对象添加域
            doc.add(new StringField("id",book.getId() + "", Field.Store.YES));
            doc.add(new TextField("bookName",book.getBookName(),Field.Store.YES));
            doc.add(new DoubleField("bookPrice",book.getPrice(),Field.Store.YES));
            doc.add(new StoredField("bookPic",book.getPic()));
            doc.add(new TextField("bookDesc",book.getBookDesc(),Field.Store.NO));
            documents.add(doc);
        }
        //创建分词器
        IKAnalyzer analyzer = new IKAnalyzer();
        //创建索引库配置对象
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);
        //设置索引库打开模式
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        //设置索引库目录对象
        FSDirectory directory = FSDirectory.open(new File("D:\\index"));
        //创建索引操作对象
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        //循环文档,写入索引库
        for (Document document : documents) {
            //把文档对象写入索引库
            indexWriter.addDocument(document);
            System.out.println("233");
            //提交事务
            indexWriter.commit();
        }
        //释放资源
        indexWriter.close();
    }

    @Test
    public void searchIndex()throws Exception{
        //创建分词器
        IKAnalyzer analyzer = new IKAnalyzer();
        //创建查询解析器对象,解释对象,得到查询对象
        Query query = new QueryParser("bookName", analyzer).parse("bookName:java");
        //创建索引目录
        FSDirectory directory = FSDirectory.open(new File("D:\\index"));
        //创建读取索引库对象
        DirectoryReader indexReader = DirectoryReader.open(directory);
        //执行索引库对象
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        //执行搜索
        TopDocs topDocs = indexSearcher.search(query, 10);
        //收集结果集
        System.out.println("总命中记录数:" + topDocs.totalHits);
        //获取搜索得到文档数组
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        //遍历文档
        for (ScoreDoc scoreDoc : scoreDocs) {
            System.out.println("==================================");
            System.out.println("文档id: " + scoreDoc.doc + "\t文档分值: " + scoreDoc.score);
            //创建文档对象
            Document doc = indexSearcher.doc(scoreDoc.doc);
            //根据文档id获取指定文档
            System.out.println("图书id: " + doc.get("id"));
            System.out.println("图书名称: " + doc.get("bookName"));
            System.out.println("图书价格: " + doc.get("bookPrice"));
            System.out.println("图书图片: " + doc.get("bookPic"));
            System.out.println("图书描述: " + doc.get("bookDesc"));
        }
        //释放资源
        indexReader.close();
    }

    @Test
    public void deleteIndexByTerm() throws Exception{
        // 创建分析器，用于分词
        IKAnalyzer analyzer = new IKAnalyzer();
        // 创建索引库配置信息对象
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);
        // 创建索引库存储目录
        FSDirectory directory = FSDirectory.open(new File("D:\\index"));
        // 创建IndexWriter，操作索引库
        IndexWriter indexWriter = new IndexWriter(directory, iwc);
        // 创建条件对象
        Term term = new Term("bookName", "java");
        // 使用IndexWriter对象，执行删除
        indexWriter.deleteDocuments(term);
        // 关闭，释放资源
        indexWriter.close();
    }

    @Test
    public void deleteIndexTermAll()throws Exception{
        IKAnalyzer analyzer = new IKAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);
        FSDirectory directory = FSDirectory.open(new File("D:\\index"));
        IndexWriter indexWriter = new IndexWriter(directory, iwc);
        indexWriter.deleteAll();
        indexWriter.close();
    }

    @Test
    public void updateIndexTerm()throws Exception{
        IKAnalyzer analyzer = new IKAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);
        FSDirectory directory = FSDirectory.open(new File("D:\\index"));
        IndexWriter indexWriter = new IndexWriter(directory, iwc);
        Document doc = new Document();
        doc.add(new StringField("id","9529",Field.Store.YES));
        doc.add(new TextField("name","lucene solr dubbo zookeeper",Field.Store.YES));
        Term term = new Term("name", "lucene");
        indexWriter.updateDocument(term,doc);
        indexWriter.commit();
        indexWriter.close();
    }

    public void search(Query query)throws Exception{
        System.out.println("查询语法" + query);
        FSDirectory directory = FSDirectory.open(new File("D:\\index"));
        DirectoryReader directoryReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);
        TopDocs search = indexSearcher.search(query, 10);
        System.out.println("总命中数: " + search.totalHits);
        ScoreDoc[] scoreDocs = search.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            System.out.println("================");
            System.out.println("文档id" + scoreDoc.doc + "\t" + scoreDoc.score);
            Document doc = indexSearcher.doc(scoreDoc.doc);
            System.out.println("图书id" + doc.get("id"));
            System.out.println("图书名称" + doc.get("bookName"));
            System.out.println("图书价格" + doc.get("bookPrice"));
            System.out.println("图书图片" + doc.get("bookPic"));
            System.out.println("图书描述" + doc.get("bookDesc"));
        }
        directoryReader.close();
    }

    @Test
    public void testTempQuery() throws Exception {
        TermQuery q = new TermQuery(new Term("bookName", "java"));
        search(q);
    }

    @Test
    public void testNumericRangeQuery() throws Exception{
        NumericRangeQuery<Double> bookPrice = NumericRangeQuery.newDoubleRange("bookPrice", 80d, 100d, false, false);
        search(bookPrice);
    }

    @Test
    public void testNumericRangeQuery2() throws Exception{
        NumericRangeQuery<Double> bookPrice = NumericRangeQuery.newDoubleRange("bookPrice", 80d, 100d, true, true);
        search(bookPrice);
    }

    @Test
    public void testBooleanQuery() throws Exception{
        TermQuery q1 = new TermQuery(new Term("bookName", "java"));
        NumericRangeQuery<Double> q2 = NumericRangeQuery.newDoubleRange("bookPrice", 80d, 100d, true, true);
        BooleanQuery q = new BooleanQuery();
        q.add(q1,BooleanClause.Occur.MUST);
        q.add(q2,BooleanClause.Occur.MUST);
        search(q);
    }

    @Test
    public void testQueryParser() throws Exception{
        // 创建分析器，用于分词
        Analyzer analyzer = new IKAnalyzer();
        // 创建QueryParser解析对象
        QueryParser queryParser = new QueryParser("bookName",analyzer);
        // 解析表达式，创建Query对象
        // +bookName:java +bookName:lucene
        Query q = queryParser.parse("bookName:java AND bookName:lucene");
        // 执行搜索
        search(q);
    }
}
