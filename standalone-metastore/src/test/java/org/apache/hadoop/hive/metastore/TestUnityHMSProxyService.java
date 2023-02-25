package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Locale;

@Category(MetastoreCheckinTest.class)
public class TestUnityHMSProxyService extends TestRemoteHiveMetaStore {

    @Override
    public void start() throws Exception {
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_TRANSPORT_MODE, "http");
        super.start();
    }

    @Override
    protected HiveMetaStoreClient createClient() throws Exception {
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE, "http");
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_AUTH_MODE, "JWT");
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_SERVER_HTTP_URL, "<http endpoint here>");
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.USE_SSL, false);
        return super.createClient();
    }

    @Test
    public void testSimple() throws Exception {
        Database db = new DatabaseBuilder()
                .setName("testUnityDb")
                .build(conf);
        client.createDatabase(db);
        Database retDb = client.getDatabase("testUnityDb");
        Assert.assertNotNull(retDb);
        Assert.assertEquals("testunitydb", retDb.getName().toLowerCase(Locale.ROOT));
    }

    @Test
    public void testGetUnityDatabase() throws Exception {
        Database db = client.getDatabase("test_schema");
        Assert.assertNotNull(db);
        Assert.assertEquals("test_schema", db.getName());
        Assert.assertEquals("unityhmsproxy", db.getCatalogName());
    }
}
