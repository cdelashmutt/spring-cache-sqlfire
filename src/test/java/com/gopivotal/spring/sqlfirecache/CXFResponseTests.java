package com.gopivotal.spring.sqlfirecache;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.impl.ResponseImpl;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.gopivotal.spring.sqlfirecache.serialized.NonSerializableBook;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class CXFResponseTests
{

	Logger log = LoggerFactory.getLogger(CXFResponseTests.class);

	@Autowired
	private CacheManager manager;

	@After
	public void clearCache()
	{
		manager.getCache("responses").clear();
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
		assertNotNull(manager.getCache("responses"));
	}

	@Test
	public void testPut()
	{
		Cache responses = manager.getCache("responses");
		Response response = ResponseImpl.ok(new NonSerializableBook(1, "Test Book")).build();
		responses.put(1, response);
		Response response2 = (Response)responses.get(1).get();
		assertThat(response.getMetadata(), equalTo(response2.getMetadata()));
		assertThat(response.getStatus(), equalTo(response2.getStatus()));
		assertThat(response.getEntity(), equalTo(response2.getEntity()));
	}
}