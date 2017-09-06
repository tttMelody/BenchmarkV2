package config;

public class Config {
    public static final String USERNAME = "ADMIN";
    public static final String PASSWORD = "KYLIN";
    public static final String HOST = "106.75.137.52";
    //    public static final String HOST = "sandbox";
    //    public static final String HOST = "localhost";
    //    public static final int PORT = 7070;
    public static final int PORT = 7370;
    public static final long TEST_DURATION = 50000;
    public static final String[] sqls = new String[] { "select count(*) from a" };
    public static final String project = "CDR_TEST";
}
