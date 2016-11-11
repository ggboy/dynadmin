var strict
module.exports = function(dynamodb) {


    function dynamo_model(schema) {

        var table_name = schema["table_name"];
        var hash_key = schema["hash_key"];
        var range_key = schema["range_key"];
        var fields = schema["fileds"]
        var indexes = schema["indexes"]

        function initParams(){
            return {
                TableName: schema["table_name"]
            }
        }


        function getTableName() {
            return table_name;
        }

        function getSchema() {
            return schema;
        }


        function globalSecondaryIndexes(){
            var global_secondary_indexes = [];
            for(var i=0; i < schema["indexes"].length ; i++){
                if(schema["indexes"][i][0] != schema["hash_key"]){
                    var rng = null;
                    if(schema["indexes"][i].length == 2){
                        global_secondary_indexes.push({"index_name": (schema["indexes"][i][0] + "_" + schema["indexes"][i][1] + "_index"),
                                                       "hash_key": schema["indexes"][i][0], "range_key": schema["indexes"][i][1]});
                    } else {
                        global_secondary_indexes.push({"index_name": (schema["indexes"][i][0] + "_index"),
                                                       "hash_key": schema["indexes"][i][0]});

                    }
                }
            }
            return global_secondary_indexes;
        };



        function localSecondaryIndexes(){
            var local_secondary_indexes = [];
            for(var i=0; i < schema["indexes"].length ; i++){
                if(schema["indexes"][i].length == 2 && schema["indexes"][i][0] == schema["hash_key"]){
                    local_secondary_indexes.push({"index_name": (schema["indexes"][i][0] + "_" + schema["indexes"][i][1] + "_index"),
                                                  "hash_key": schema["indexes"][i][0], "range_key": schema["indexes"][i][1]});
                }
            }
            return local_secondary_indexes
        };


        function createTable(clb){
            var params = initParams();
            params["AttributeDefinitions"] = [];
            var required_fields = [];
            required_fields.push(schema["hash_key"]);
            if(schema["range_key"]){
                required_fields.push(schema["range_key"]);
            }
            for(var i=0; i< schema["indexes"].length ; i++){
                for(var j=0 ; j<schema["indexes"][i].length; j++){
                    if(required_fields.indexOf(schema["indexes"][i][j]) == -1){
                        required_fields.push(schema["indexes"][i][j]);
                    }
                }
            }
            for(var i=0; i<required_fields.length; i++){
                params["AttributeDefinitions"].push( { AttributeName: required_fields[i], 
                                                       AttributeType: schema["fields"][required_fields[i]]});
                
            }
            params["KeySchema"] = [];
            params["KeySchema"].push({
                AttributeName: schema["hash_key"],
                KeyType: 'HASH'
            });
            if(schema["range_key"]){
                params["KeySchema"].push({
                    AttributeName: schema["range_key"],
                    KeyType: 'RANGE'
                });
            }
            params["ProvisionedThroughput"] = {
                ReadCapacityUnits: 6,
                WriteCapacityUnits: 5 
            };
            
            params["GlobalSecondaryIndexes"] = [];
            var gsis = globalSecondaryIndexes();
            var lsis = localSecondaryIndexes();
            for(var i=0; i<gsis.length; i++){
                params["GlobalSecondaryIndexes"].push({
                    IndexName: gsis[i]['index_name'],
                    KeySchema: [
                        {
                            AttributeName: gsis[i]['hash_key'],
                            KeyType: 'HASH'
                        }
                    ]
                });
                if(gsis[i]['range_key']){
                    params["GlobalSecondaryIndexes"][i]['KeySchema'].push({
                        AttributeName: gsis[i]['range_key'],
                        KeyType: 'RANGE'
                    });
                }
                params["GlobalSecondaryIndexes"][i]['ProvisionedThroughput']={
                    ReadCapacityUnits: 6,
                    WriteCapacityUnits: 5 
                };
                params["GlobalSecondaryIndexes"][i]['Projection'] = {
                    ProjectionType: 'ALL'
                };      
            }
            
            params["LocalSecondaryIndexes"] = [];    
            for(var i=0; i<lsis.length; i++){
                params["LocalSecondaryIndexes"].push({
                    IndexName: lsis[i]['index_name'],
                    KeySchema: [
                        {
                            AttributeName: lsis[i]['hash_key'],
                            KeyType: 'HASH'
                        },
                        {
                            AttributeName: lsis[i]['range_key'],
                            KeyType: 'RANGE'
                        }
                    ],
                    Projection:  {
                        ProjectionType: 'ALL'
                    }
                });
            }
            if(gsis.length == 0){
                delete(params["GlobalSecondaryIndexes"]);
            }
            if(lsis.length == 0){
                delete(params["LocalSecondaryIndexes"]);
            }
            
            logger.info(JSON.stringify(params));
            dynamodb.createTable(params, function(err, data){
                clb(err, data);
            })

        }


        function findOne(hsh, rng, clb){
            var redis_key = redisKey(hsh, rng)
            redis.get(redis_key, function(err, reply) {
                err = 'yes'
	        if(!err && reply) {
                    clb(err, JSON.parse(reply));	  
	        } else {
	            var params = initParams();
	            params["Key"] = formatParams(schema["hash_key"], hsh);
	            if(rng){
                        var attrs = formatParams(schema["range_key"], rng);
                        for(var attr in  attrs){
                            params["Key"][attr] = attrs[attr]
                        }
	            }
	            dynamodb.getItem(params,function(err, data){
                        var obj = {};
                        if(err == null){
                            obj = makeObject(data['Item']);	  
                        }
	                if(Object.keys(obj) && Object.keys(obj).length > 0) {
	                    redis.set(redis_key, JSON.stringify(obj), function(err, reply) {
		                if(err) {
		                    logger.log(err)
		                } else {
		                    logger.log('cache hit....');
	                        }
	                    });
	                }
                        if(Object.keys(obj).length === 0) {
                            err = {err: "Object Not found"}
                            obj = null;
                        }
	                clb(err, obj)
	            });	  
	        }	
            });
            
        };

        
        function findMany(key, val, options, clb){
            var params = initParams();
            if(index != null) { 
                params["IndexName"] = index
            }
            params['KeyConditions'] = {}
            params['KeyConditions'][key] = {}
            params['KeyConditions'][key]['ComparisonOperator'] = 'EQ';
            params['KeyConditions'][key]['AttributeValueList'] = [ fmtParam(key, val) ]
            dynamodb.query(params, function(err, data){
                var objs = [];
                if(err == null){
                    for(var i=0; i< data["Items"].length; i++){            
                        objs.push(makeObject(data["Items"][i]));
                    }
                }
                clb(err, objs);
            });                    
        }


        function create(obj, clb) {
            
        }



        function update(obj, clb) {


            
        }

        function destroy(obj, clb) {

            
        }



        return {
            getTableName: getTableName,
            getSchema: getSchema,
            createTable: createTable,
            findOne: findOne,
            create: create,
            update: update,
            destroy: destroy,
            localsecondaryindexes: localsecondaryindexes,
            globalsecondaryindexes: globalsecondaryindexes
        }



    }

    
}
