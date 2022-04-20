public class NdbClientGetTest {
    public static void main(String[] args) {
        System.out.println("WZR PATH: " + System.getProperty("java.library.path"));
        System.loadLibrary("ndbclient");
    }
}