package com.gopivotal.spring.sqlfirecache;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import com.gopivotal.spring.sqlfirecache.serialized.Book;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class XMLConfigurationTests
{

	Logger log = LoggerFactory.getLogger(XMLConfigurationTests.class);

	@Autowired
	private CacheManager manager;

	@Autowired
	private Repository<Book, Integer> bookRepository;

	@After
	public void clearCache()
	{
		manager.getCache("books").clear();
	}

	@Test
	public void testManagerCreated()
		throws Exception
	{
		assertNotNull(manager);
	}

	@Test
	public void testManagerHasCache()
		throws Exception
	{
		assertNotNull(manager.getCache("books"));
	}

	@Test
	public void testSave()
	{
		StopWatch sw = new StopWatch("testSave");
		sw.start();
		bookRepository.save(new Book(1, "Lord of the Rings"));
		sw.stop();
		log.info(sw.prettyPrint());
	}

	@Test
	public void testDelete()
	{
		Book book = new Book(1, "Lord of the Rings");
		bookRepository.save(book);
		bookRepository.delete(book);
	}

	@Test
	public void testGetById()
	{
		Book book = new Book(1, "Lord of the Rings");
		bookRepository.save(book);
		Book book2 = bookRepository.getById(book.getId());
		assertThat(book, equalTo(book2));
	}

	@Test
	public void testCache()
	{
		Book book = new Book(null, "Lord of the Rings");
		book = bookRepository.save(book);
		StopWatch sw = new StopWatch("testCache");
		sw.start("uncached");
		bookRepository.getById(book.getId());
		sw.stop();
		sw.start("cached");
		bookRepository.getById(book.getId());
		sw.stop();
		log.info(sw.prettyPrint());
	}

	@Test
	public void testCacheEvict()
	{
		// Saving with an ID will automatically cache the book
		Book book = new Book(1, "Lord of the Rings");
		book = bookRepository.save(book);
		StopWatch sw = new StopWatch("testCacheEvict");
		sw.start("before delete");
		bookRepository.getById(book.getId());
		sw.stop();
		bookRepository.delete(book);
		sw.start("after delete");
		Book book2 = bookRepository.getById(book.getId());
		sw.stop();
		assertThat(book2, nullValue());
		log.info(sw.prettyPrint());
	}

}