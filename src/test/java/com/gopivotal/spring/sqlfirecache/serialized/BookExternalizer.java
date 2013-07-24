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
package com.gopivotal.spring.sqlfirecache.serialized;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.gopivotal.spring.sqlfirecache.externalizer.Externalizer;

/**
 * A sample externalizer capable of serializing a non-serializable book.
 * 
 * @author cdelashmutt
 */
public class BookExternalizer
	implements Externalizer<NonSerializableBook>
{

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.Externalizer#writeObject(java.io.
	 * ObjectOutputStream, java.lang.Object)
	 */
	@Override
	public void writeObject(ObjectOutputStream stream, NonSerializableBook book)
		throws IOException
	{
		stream.writeInt(book.getId());
		stream.writeObject(book.getTitle());
		stream.writeObject(book.getAuthor());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.Externalizer#readObject(java.io.
	 * ObjectInputStream)
	 */
	@Override
	public NonSerializableBook readObject(ObjectInputStream stream)
		throws IOException, ClassNotFoundException
	{
		NonSerializableBook value = new NonSerializableBook();
		
		value.setId(stream.readInt());
		value.setTitle((String)stream.readObject());
		value.setAuthor((String)stream.readObject());
		
		return value;
	}

}
