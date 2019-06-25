package chenbo.cimiss;

import chenbo.cimiss.transfer.TransferController;
import chenbo.cimiss.transfer.TransferTask;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by chenbo on 2019/3/31.
 */

public class TestTransfer {
    private TransferTask task;

    private TransferController controller;

    @Before
    public void init(){
        controller  = new TransferController("db-transfer.cfg");
    }

    @Test
    public void testOne(){
        TransferTask task = controller.genTask("test1");

        task.run();
    }
}
