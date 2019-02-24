package cn.itcast.lucene.dao.impl;

import cn.itcast.lucene.dao.BookDao;
import cn.itcast.lucene.pojo.Book;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class BookImpl implements BookDao {
    @Override
    public List<Book> findAll() {
        ArrayList<Book> bookList = new ArrayList<>();
        Connection connection = null;
        PreparedStatement psmt = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/day34","root","root");
            psmt = connection.prepareStatement("SELECT * FROM book");
            rs = psmt.executeQuery();
            while (rs.next()){
                Book book = new Book();
                book.setId(rs.getInt("id"));
                book.setBookDesc(rs.getString("bookdesc"));
                book.setPrice(rs.getFloat("price"));
                book.setPic(rs.getString("pic"));
                book.setBookName(rs.getString("bookname"));
                bookList.add(book);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (rs != null) rs.close();
                if (psmt != null) psmt.close();
                if (connection != null) connection.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return bookList;
    }
}
