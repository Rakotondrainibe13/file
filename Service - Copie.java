import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.elasticsearch.client.RestClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import org.apache.http.impl.client.BasicCredentialsProvider;


public class Service {
    String login="glenn";
    String password="glenn123";
    String host="localhost";
    int port=9200;

    /* 
    **  Connect Elasticsearch
    */
    /**
     * @return
     * @throws IOException
     */
    public ElasticsearchClient getConElasticsearch() throws Exception{
        File certFile = new File("C:/elastic/elasticsearch-8.4.3/config/certs/http_ca.crt");

        SSLContext sslContext = TransportUtils
            .sslContextFromHttpCaCrt(certFile); 

        BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
        credsProv.setCredentials(
            AuthScope.ANY, new UsernamePasswordCredentials(this.login, this.password)
        );

        final RestClient restClient = RestClient
            .builder(new HttpHost(this.host, this.port, "https")) 
            .setHttpClientConfigCallback(hc -> hc
                .setSSLContext(sslContext) 
                .setDefaultCredentialsProvider((CredentialsProvider) credsProv)
            )
            .build();

        // Create the transport and the API client
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient client = new ElasticsearchClient(transport);
        return client;
    }

    
    /* 
    **  doBulk Elasticsearch
    */
    public void doBulk(ElasticsearchClient esClient) throws Exception{
        List<Key> keys = fetchkeys();

        BulkRequest.Builder br = new BulkRequest.Builder();
        int nb=0;
        for (Key key : keys) {
            br.operations(op -> op           
                .index(idx -> idx            
                    .index("keys_test")       
                    .id(String.valueOf(key.getId()))
                    .document(key)
                )
            );
            nb++;
            if(nb%20==0) System.out.println(String.valueOf(nb)+" of "+String.valueOf(keys.size()));
        }

        BulkResponse result = esClient.bulk(br.build());

        // Log errors, if any
        // if (result.errors()) {
        //     logger.error("Bulk had errors");
        //     for (BulkResponseItem item: result.items()) {
        //         if (item.error() != null) {
        //             logger.error(item.error().reason());
        //         }
        //     }
        // }
    }

    public List<Key> fetchkeys() throws Exception {
        Connection con = new Connectbd().connectto();
        String req="SELECT"
        +"`Key` .id,"
        +"Max(`Key` .name),"
        +"Max(`Key` .description),"
        +"Max(`Key` .status),"
        +"Max(`Category`.`en`) AS `en`,"
        +"Max(`Category`.`fr`) AS `fr`,"
        +"Max(`Category`.`pt`) AS `pt`,"
        +"Max(`Category`.`de`) AS `de`,"
        +"Max(`KeyLocation->Location`.`codeName`) AS `codeName`,"
        +"REPLACE ( Max(`KeyLocation->Location`.`codeName`),'-','') AS `codeNameShort`,"
        +"Max(`KeysGroups->KeyManagementUser->UserGroup->Users`.`preference`) AS `preference`,"
        +"Max(`KeyHolding->User`.`firstName`) AS `firstname`,"
        +"Max(`KeyHolding->User`.`lastName`) AS `lastname`,"
        +"Max(`KeysGroups->KeyManagementUser->UserGroup->Users->users_group`.`userId`) AS `userid`,"
        +"Max(`KeysGroups->KeyManagementUser->UserGroup->Users->users_group`.`groupId`) AS `groupid`,"
        +"unix_timestamp(Max(`Key`.updatedAt)) as `unix_ts_in_secs`"
        +"FROM"
        +"("
        +"SELECT"
        +"    `Key`.`id`,"
        +"    `Key`.`backupId`,"
        +"    `Key`.`name`,"
        +"    `Key`.`description`,"
        +"    `Key`.`status`,"
        +"    `Key`.`picturePath`,"
        +"    `Key`.`archivedAt`,"
        +"    `Key`.`hasAllRights`,"
        +"    `Key`.`acsesAccessId`,"
        +"    `Key`.`acsesCodes`,"
        +"    `Key`.`acsesObjectId`,"
        +"    `Key`.`parentId`,"
        +"    `Key`.`createdAt`,"
        +"    `Key`.`updatedAt`,"
        +"    `Key`.`deletedAt`,"
        +"    `Key`.`ownerId`,"
        +"    `Key`.`categoryId`"
        +"FROM"
        +"    `keys` AS `Key`"
        +"WHERE"
        +"    (`Key`.`deletedAt` IS NULL"
        +"    AND (`Key`.`status` != 'PENDING_CREATION'"
        +" AND (`Key`.`archivedAt` IS NULL"
        +" OR `Key`.`hasAllRights` = true)))"
        +"    AND ("
        +"    SELECT"
        +"    `keys_groups`.`keyId`"
        +"    FROM"
        +"    `keys_groups` AS `keys_groups`"
        +"    INNER JOIN `groups` AS `Group` ON"
        +"    `keys_groups`.`groupId` = `Group`.`id`"
        +"    AND (`Group`.`deletedAt` IS NULL)"
        +"    INNER JOIN `key_management` AS `Group->KeyManagementUser` ON"
        +"    `Group`.`id` = `Group->KeyManagementUser`.`keyGroupId`"
        +" AND (`Group->KeyManagementUser`.`deletedAt` IS NULL)"
        +"    WHERE"
        +" (`Key`.`id` = `keys_groups`.`keyId`)"
        +"    LIMIT 1 ) IS NOT NULL"
        +"ORDER BY"
        +"    `Key`.`updatedAt` DESC"
        +") AS `Key`"
        +"LEFT OUTER JOIN `key_location` AS `KeyLocation` ON"
        +"`Key`.`id` = `KeyLocation`.`keyId`"
        +"AND (`KeyLocation`.`deletedAt` IS NULL"
        +"    AND `KeyLocation`.`endDate` IS NULL)"
        +"LEFT OUTER JOIN `locations` AS `KeyLocation->Location` ON"
        +"`KeyLocation`.`locationId` = `KeyLocation->Location`.`id`"
        +"AND (`KeyLocation->Location`.`deletedAt` IS NULL)"
        +"LEFT OUTER JOIN `key_holding` AS `KeyHolding` ON"
        +"`Key`.`id` = `KeyHolding`.`keyId`"
        +"AND (`KeyHolding`.`deletedAt` IS NULL"
        +"    AND `KeyHolding`.`endDate` IS NULL)"
        +"LEFT OUTER JOIN `users` AS `KeyHolding->User` ON"
        +"`KeyHolding`.`userId` = `KeyHolding->User`.`id`"
        +"AND (`KeyHolding->User`.`deletedAt` IS NULL)"
        +"LEFT OUTER JOIN `boxes` AS `KeyLocation->Location->Box` ON"
        +"`KeyLocation->Location`.`boxId` = `KeyLocation->Location->Box`.`id`"
        +"AND (`KeyLocation->Location->Box`.`deletedAt` IS NULL)"
        +"LEFT OUTER JOIN `relays` AS `KeyLocation->Location->Box->Relay` ON"
        +"`KeyLocation->Location->Box`.`relayId` = `KeyLocation->Location->Box->Relay`.`id`"
        +"AND (`KeyLocation->Location->Box->Relay`.`deletedAt` IS NULL)"
        +"LEFT OUTER JOIN `categories` AS `Category` ON"
        +"`Key`.`categoryId` = `Category`.`id`"
        +"INNER JOIN ( `keys_groups` AS `KeysGroups->keys_groups`"
        +"INNER JOIN `groups` AS `KeysGroups` ON"
        +"`KeysGroups`.`id` = `KeysGroups->keys_groups`.`groupId`) ON"
        +"`Key`.`id` = `KeysGroups->keys_groups`.`keyId`"
        +"AND (`KeysGroups`.`deletedAt` IS NULL)"
        +"INNER JOIN `key_management` AS `KeysGroups->KeyManagementUser` ON"
        +"`KeysGroups`.`id` = `KeysGroups->KeyManagementUser`.`keyGroupId`"
        +"AND (`KeysGroups->KeyManagementUser`.`deletedAt` IS NULL)"
        +"LEFT OUTER JOIN `groups` AS `KeysGroups->KeyManagementUser->UserGroup` ON"
        +"`KeysGroups->KeyManagementUser`.`userGroupId` = `KeysGroups->KeyManagementUser->UserGroup`.`id`"
        +"AND (`KeysGroups->KeyManagementUser->UserGroup`.`deletedAt` IS NULL)"
        +"LEFT OUTER JOIN ( `users_group` AS `KeysGroups->KeyManagementUser->UserGroup->Users->users_group`"
        +"INNER JOIN `users` AS `KeysGroups->KeyManagementUser->UserGroup->Users` ON"
        +"`KeysGroups->KeyManagementUser->UserGroup->Users`.`id` = `KeysGroups->KeyManagementUser->UserGroup->Users->users_group`.`userId`) ON"
        +"`KeysGroups->KeyManagementUser->UserGroup`.`id` = `KeysGroups->KeyManagementUser->UserGroup->Users->users_group`.`groupId`"
        +"AND (`KeysGroups->KeyManagementUser->UserGroup->Users`.`deletedAt` IS NULL) Group By `Key` .id";
        java.sql.Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery(req);
       List<Key> listret=new ArrayList<Key>();
       while(res.next())
        {
            Key enc=new Key();
            enc.setId(res.getInt(1));
            enc.setName(res.getString(2));
            enc.setDescription(res.getString(3));
            enc.setStatus(res.getString(4));
            enc.setEn(res.getString(5));
            enc.setFr(res.getString(6));
            enc.setPt(res.getString(7));
            enc.setPt(res.getString(7));
            enc.setCodeName(res.getString(9));
            enc.setCodeNameShort(res.getString(10));
            enc.setPreference(res.getString(11));
            enc.setFirstname(res.getString(12));
            enc.setLastname(res.getString(13));
            enc.setUserid(res.getInt(14));
            enc.setGroupid(res.getInt(15));
            enc.setUnix_ts_in_secs(res.getInt(16));
            listret.add(enc);
        }
        
        con.close();
        return listret;
    }
}