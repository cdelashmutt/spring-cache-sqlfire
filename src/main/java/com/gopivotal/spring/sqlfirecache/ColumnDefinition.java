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
package com.gopivotal.spring.sqlfirecache;

/**
 * A column definition.
 * 
 * @author cdelashmutt
 */
public class ColumnDefinition
{
	private String name;

	private SQLFType type;

	private Integer length;

	private LengthUnit unit;

	private Integer precision;

	private Integer scale;

	public ColumnDefinition()
	{
		super();
	}
	
	/**
	 * Constructs a ColumnDefinition with a name, type and defaults of other values
	 *
	 * @param name The name to use for the column
	 * @param type The data type of the column
	 */
	public ColumnDefinition(String name, SQLFType type)
	{
		super();
		this.name = name;
		this.type = type;
	}

	/**
	 * Constructs a ColumnDefinition with a name, type and length
	 *
	 * @param name The name to use for the column
	 * @param type The data type of the column
	 * @param length The length to use for the field
	 */
	public ColumnDefinition(String name, SQLFType type, Integer length)
	{
		this(name, type);
		this.length = length;
	}

	/**
	 * Constructs a ColumnDefinition with a name, type, length and unit
	 *
	 * @param name The name to use for the column
	 * @param type The data type of the column
	 * @param length The length to use for the field
	 * @param unit
	 */
	public ColumnDefinition(String name, SQLFType type, Integer length,
			LengthUnit unit)
	{
		this(name, type, length);
		this.unit = unit;
	}

	/**
	 * Constructs a ColumnDefinition with a name, type, precision and scale
	 *
	 * @param name The name to use for the column
	 * @param type The data type of the column
	 * @param precision The precision of the column
	 * @param scale The scale of the column
	 */
	public ColumnDefinition(String name, SQLFType type, Integer precision,
			Integer scale)
	{
		this(name, type);
		this.precision = precision;
		this.scale = scale;
	}

	/**
	 * @return the name
	 */
	public String getName()
	{
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name)
	{
		this.name = name;
	}

	/**
	 * @return the type
	 */
	public SQLFType getType()
	{
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(SQLFType type)
	{
		this.type = type;
	}

	/**
	 * @return the length
	 */
	public Integer getLength()
	{
		return length;
	}

	/**
	 * @param length
	 *            the length to set
	 */
	public void setLength(Integer length)
	{
		this.length = length;
	}

	/**
	 * @return the precision
	 */
	public Integer getPrecision()
	{
		return precision;
	}

	/**
	 * @param precision
	 *            the precision to set
	 */
	public void setPrecision(Integer precision)
	{
		this.precision = precision;
	}

	/**
	 * @return the scale
	 */
	public Integer getScale()
	{
		return scale;
	}

	/**
	 * @param scale
	 *            the scale to set
	 */
	public void setScale(Integer scale)
	{
		this.scale = scale;
	}

	/**
	 * @return the unit
	 */
	public LengthUnit getUnit()
	{
		return unit;
	}

	/**
	 * @param unit
	 *            the unit to set
	 */
	public void setUnit(LengthUnit unit)
	{
		this.unit = unit;
	}

	/**
	 * Creates an SQL column type definition fragment suitable for use in a CREATE TABLE statement.
	 *
	 * @return A type string with no column name that is suitable for appending to the column name.
	 */
	public String buildColumnTypeDefinitionSQL()
	{
		String columnDef = this.getType().getSQLName();
		switch (this.getType())
		{
			//Possibly Length, and Suffix needed
			case BINARY:
			case VARBINARY:
				if(this.getLength() != null)
					columnDef += "(" + this.getLength() + ")";
				columnDef += " FOR BIT DATA";
				break;
				
			// Length and Unit
			case BLOB:
			case CLOB:
				if(this.getLength() != null)
				{
					columnDef += "(" + this.getLength();
					if(this.getUnit() != null)
					{
						columnDef += this.getUnit().name();
					}
					columnDef += ")";
				}
				break;
				
			//Length only
			case CHAR:
			case VARCHAR:
				if(this.getLength() != null)
				{
					columnDef += "(" + this.getLength() + ")";
				}
				break;
				
			//Precision and Scale
			case DECIMAL:
			case NUMERIC:
				if(this.getPrecision() != null)
				{
					columnDef += "(" + this.getPrecision();
					if(this.getScale() != null)
					{
						columnDef += ", " + this.getScale();
					}
					columnDef += ")";
				}
				break;
				
			//Nothing else needs to happen.
			default:
				break;
		}
		return columnDef;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "ColumnDefinition [name=" + name + ", type=" + type
				+ ", length=" + length + ", unit=" + unit + ", precision="
				+ precision + ", scale=" + scale + "]";
	}
}
