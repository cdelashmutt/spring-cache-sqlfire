Support classes to allow SQLFire to be a cache provider for Spring's Caching Abstraction

This project supplies a ConfigurableColumnDefinedSQLFireCache which can be used for many 
cases by simply defining the column configuration to match the bean property names for 
the key and value objects that are going to be stored in the cache.

Also, you can use the AbstractColumnDefinedSQLFireCache as a base for you own column 
defined storage strategies, or AbstractSQLFireCache to supply your own SQL.