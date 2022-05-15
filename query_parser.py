from constant import *
import sqlparse, json

class parse_queries(object):
    def __init__(self, queries):
        self.queries = queries

    def get_final_message(self):
        message = {
            VERSION : self.get_version(),
            "message": self.get_message()
        }
        return message

    def get_version(self):
        version = {
            VERSION:"1.0.0",
            VERSION_PARTS :[1]
            }
        return version

    def get_message(self):
        query_list = sqlparse.split(self.queries)
        entity_json_list = []

        for query in query_list:
            type = self.get_type(query)
            if(type == CREATE_DATABASE):
                database_name = query.split(" ")[2]
                entity_json_list.append(self.get_create_database_entity(database_name)) 
            elif(type == DROP_DATABASE):
                pass
            elif(type == CREATE_TABLE):
                pass
            elif(type == DROP_TABLE):
                pass
            elif(type == ALTER_TABLE):
                pass
            else:
                pass

        message = {
            TYPE: "ENTITY_CREATE_V2",
            USER: "nandani",
            ENTITIES :{
                ENTITIES : entity_json_list
                }
            }
        return json.dumps(message)

    def get_type(self, query):
        type = (' '.join(query.split(" ")[0:2])).upper()
        return type

    def get_create_database_entity(self, database_name):
        entity_json = {
            ENTITY:{
                TYPE_NAME: "rdbms_db",
                ATTRIBUTES: {
                    QUALIFIED_NAME: database_name,
                    NAME: database_name
                    },
                RELATIONSHIP_ATTRIBUTES: {
                    INSTANCE: {
                        TYPE_NAME: "rdbms_instance",
                        UNIQUE_ATTRIBUTES:{
                            QUALIFIED_NAME: "postgres"
                        },
                        RELATIONSHIP_TYPE: "rdbms_instance_databases"
                    }
                }
            }
        }
        return entity_json






        


