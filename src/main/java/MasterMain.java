
public class MasterMain {
//    public static final Logger logger = Logger.getLogger(MasterMain.class);
    public static void main(String[] args) throws Exception {
        int length = args.length;
        String[] subarray = new String[length-1];
        System.arraycopy(args, 1, subarray, 0, subarray.length);
        String job = args[0];
        if (job.equals("WN")) {
            mr.joinwn.Main.main(subarray);
        }
    }
}
