import config.Sqls;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

public class StoredAsSQLFile {
    public static void main(String[] args) throws Exception {
        String whereClause1 = Sqls.MSISDN_WhereClause;
        String whereClause2 = Sqls.IMSI_WhereClause;
        Map<String, String[]> SQLWithAllCol1 = Sqls.getSQL(Sqls.selectAllColClause, whereClause1);
        Map<String, String[]> SQLWithOneCol1 = Sqls.getSQL(Sqls.selectOneColClause, whereClause1);
        Map<String, String[]> SQLWithAllCol2 = Sqls.getSQL(Sqls.selectAllColClause, whereClause2);
        Map<String, String[]> SQLWithOneCol2 = Sqls.getSQL(Sqls.selectOneColClause, whereClause2);


        write(SQLWithAllCol1);
        write(SQLWithOneCol1);

        write(SQLWithAllCol2);
        write(SQLWithOneCol2);
    }

    public static void write(Map<String, String[]> sqlMap) throws Exception {
        for (String pre : sqlMap.keySet()) {
            String[] sqls = sqlMap.get(pre);
            for (int i = 0; i < sqls.length; i++) {
                String fileName = pre + "_" + i;
                try (Writer writer = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(fileName), "utf-8"))) {
                    writer.write(sqls[i]);
                }
            }
        }
    }
}
