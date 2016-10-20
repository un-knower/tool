package com.hiido.hcat.service;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.junit.Before;
import org.junit.Test;

import com.hiido.hcat.thrift.protocol.AuthorizationException;
import com.hiido.hcat.thrift.protocol.RuntimeException;

public class QuickTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() throws AuthorizationException, RuntimeException, IOException, ParseException {
        //String queryStr = "set hive.fetch.task.conversion=none;\nset hive.execution.engine=spark;\ninsert into tta select ip,country from default.yy_mbsdkinstall_original where dt='20160601'  and ip='122.13.229.21' ;";
        String queryStr = "insert overwrite local directory '/tta' select ip,country from default.yy_mbsdkinstall_original where dt='20160601'  and ip='122.13.229.21' ;";
        boolean quick = true;
        Configuration conf = new Configuration();
        Context ctx = new Context(conf, false);
        ParseDriver pd = new ParseDriver();
        String command = "";
        for (String oneCmd : queryStr.split(";")) {
            if (StringUtils.endsWith(oneCmd, "\\")) {
                command += StringUtils.chop(oneCmd) + ";";
                continue;
            } else {
                command += oneCmd;
            }
            if (StringUtils.isBlank(command)) {
                continue;
            }
            quick = quick & isQuickCmd(command, ctx, pd);
            System.out.println(quick);
        }
    }

    private static boolean isQuickCmd(String command, Context ctx, ParseDriver pd) throws ParseException {
        String[] tokens = command.split("\\s+");
        HiveCommand hiveCommand = HiveCommand.find(tokens, false);

        // not Driver
        if (hiveCommand != null)
            return true;
        boolean isQuick = true;
        ASTNode tree = pd.parse(command, ctx);
        tree = ParseUtils.findRootNonNullToken(tree);
        System.out.println(tree.getChild(1).getChild(0).getChild(0).getChild(0).getType());
        switch (tree.getType()) {
            case HiveParser.TOK_ALTERTABLE:
            case HiveParser.TOK_ALTERVIEW:
            case HiveParser.TOK_CREATEDATABASE:
            case HiveParser.TOK_SWITCHDATABASE:
            case HiveParser.TOK_DROPTABLE:
            case HiveParser.TOK_DROPVIEW:
            case HiveParser.TOK_DESCDATABASE:
            case HiveParser.TOK_DESCTABLE:
            case HiveParser.TOK_DESCFUNCTION:
            case HiveParser.TOK_MSCK:
            case HiveParser.TOK_ALTERINDEX_REBUILD:
            case HiveParser.TOK_ALTERINDEX_PROPERTIES:
            case HiveParser.TOK_SHOWDATABASES:
            case HiveParser.TOK_SHOWTABLES:
            case HiveParser.TOK_SHOWCOLUMNS:
            case HiveParser.TOK_SHOW_TABLESTATUS:
            case HiveParser.TOK_SHOW_TBLPROPERTIES:
            case HiveParser.TOK_SHOW_CREATEDATABASE:
            case HiveParser.TOK_SHOW_CREATETABLE:
            case HiveParser.TOK_SHOWFUNCTIONS:
            case HiveParser.TOK_SHOWPARTITIONS:
            case HiveParser.TOK_SHOWINDEXES:
            case HiveParser.TOK_SHOWLOCKS:
            case HiveParser.TOK_SHOWDBLOCKS:
            case HiveParser.TOK_SHOW_COMPACTIONS:
            case HiveParser.TOK_SHOW_TRANSACTIONS:
            case HiveParser.TOK_SHOWCONF:
            case HiveParser.TOK_CREATEINDEX:
            case HiveParser.TOK_DROPINDEX:
            case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
            case HiveParser.TOK_REVOKE:
            case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
            case HiveParser.TOK_ALTERDATABASE_OWNER:
            case HiveParser.TOK_TRUNCATETABLE:
            case HiveParser.TOK_SHOW_SET_ROLE:
            case HiveParser.TOK_CREATEFUNCTION:
            case HiveParser.TOK_DROPFUNCTION:
            case HiveParser.TOK_RELOADFUNCTION:
            case HiveParser.TOK_ANALYZE:
            case HiveParser.TOK_CREATEMACRO:
            case HiveParser.TOK_DROPMACRO:
            case HiveParser.TOK_UPDATE_TABLE:
            case HiveParser.TOK_DELETE_FROM:
            case HiveParser.TOK_START_TRANSACTION:
            case HiveParser.TOK_COMMIT:
                isQuick = true;
                break;
            case HiveParser.TOK_CREATETABLE:
                for (int i = 0; i < tree.getChildCount(); i++)
                    if (tree.getChild(i).getType() == HiveParser.TOK_QUERY) {
                        isQuick = false;
                        break;
                    }
                // 默认是true，所以不需要判断
                break;
            default:
                isQuick = false;
        }
        return isQuick;
    }
}
