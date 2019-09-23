# **Too-yy, designed by yy**

## Installing and Getting started

1. Install

    The easiest way to install.
    
        pip install psql-yy
  
    Or you can clone source code from github.
  
        git clone git@github.com:guaidashu/psql_yy.git

3. Start

    Example

  	    from psql_yy import PsqlDB
  	    
  	    psql = PsqlDB(database="postgres", username="postgres")
  	    
  	    data = psql.select({
            "table": "postgres",
            "condition": ["column=1"],
            "limit": [0, 1]
        }, is_close_db=False)

        last_id = psql.insert_last_id({
            "table": "postgres",
            "username": "guaidashu",
        }, return_columns="id", is_close_db=False)
        
        sql = psql.update({
            "table": "postgres",
            "condition": ["id=21"],
            "set": {"password": "password"}
        }, is_close_db=False)
        
        result = psql.delete({
            "table": "postgres",
            "condition": ["id=12"]
        }, is_close_db=False)


## Usage

None

## FAQ

None

## Running Tests

## Finally Thanks 

Thanks for your support.