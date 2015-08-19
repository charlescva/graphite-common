/**
 * Created by cott on 8/12/15.
 */


public class ZkCli {
    private static ZkCli ourInstance = new ZkCli();

    public static ZkCli getInstance() {
        return ourInstance;
    }

    public static void main(String[] args) {

        zkCliTest test = new zkCliTest();
        //test.zknative("/storm/assignments/MyTopology-147-1437709253");
        //test.curator("/storm/assignments/MyTopology-147-1437709253");

        while(true) {
            try {
                test.nimbuscli();
                Thread.sleep(60000);
            }
            catch (InterruptedException err) {
                err.printStackTrace();
            }
        }
    }

    private ZkCli() {

    }
}
