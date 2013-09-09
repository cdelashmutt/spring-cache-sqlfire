/*
 * Licensed to the Apache Software Foundation (ASF) under one 
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gopivotal.spring.sqlfirecache.externalizer.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gopivotal.spring.sqlfirecache.externalizer.Externalizer;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

/**
 * An externalizer for CXF 2.4.2 Response objects
 *
 * @author cdelashmutt
 */
public class CXFResponseExternalizer
	implements Externalizer<Response>
{
	
	Logger logger = LoggerFactory.getLogger(CXFResponseExternalizer.class);
	
	XStream xstream = new XStream(new StaxDriver());
	
	ResponseCache responseCache = new ResponseCache();
	
	public CXFResponseExternalizer()
	{
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.externalizer.Externalizer#writeObject(java.io.ObjectOutputStream, java.lang.Object)
	 */
	@Override
	public void writeObject(ObjectOutputStream stream, Response obj)
		throws IOException
	{
		stream.writeInt(obj.getStatus());
		stream.writeObject(new HashMap<String,Object>(obj.getMetadata()));
		Object entity = obj.getEntity();
		String xml = responseCache.get(entity);
		if(xml == null)
		{
			xml = xstream.toXML(entity);
			if(logger.isTraceEnabled())
			{
				logger.trace(xml);
			}
			responseCache.put(entity, xml);
		}
		stream.writeObject(xml);
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.externalizer.Externalizer#readObject(java.io.ObjectInputStream)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Response readObject(ObjectInputStream stream)
		throws IOException, ClassNotFoundException
	{
		int status = stream.readInt();
		HashMap<String,Object> map = (HashMap<String, Object>)stream.readObject();
		ResponseBuilder builder = Response.status(status);
		for(Map.Entry<String, Object> entry : map.entrySet())
		{
			if(entry.getValue() instanceof List)
			{
				List<Object> values = (List<Object>)entry.getValue();
				for(Object value : values)
				{
					builder.header(entry.getKey(), value);
				}
			}
			else
				builder.header(entry.getKey(), entry.getValue());
		}
		Object entity = xstream.fromXML((String)stream.readObject());
		builder.entity(entity);
		return builder.build();
	}

	@SuppressWarnings("serial")
	private class ResponseCache
	extends LinkedHashMap<Object, String>
	{
		private int max = 10;
		
		/* (non-Javadoc)
		 * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
		 */
		@Override
		protected boolean removeEldestEntry(
				java.util.Map.Entry<Object, String> eldest)
		{
			return size() > max;
		}
	}
}