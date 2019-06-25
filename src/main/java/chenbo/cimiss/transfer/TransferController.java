package chenbo.cimiss.transfer;

import cn.golaxy.gkg.base.IniReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by chenbo on 2019/3/31.
 */
public class TransferController {
    private static Logger logger = LogManager.getLogger(TransferController.class);

    private IniReader reader;

    private List<TransferTask> tasks = new ArrayList<>(8);
    private List<Thread> threads = new ArrayList<>(8);

    private String defaultSrcDB, defaultDestDB;

    private String taskList = null;

    public TransferController(String config){
        reader = new IniReader(config);
        this.defaultSrcDB = reader.getGlobalValue("db.src");
        this.defaultDestDB = reader.getGlobalValue("db.dest");
    }

    public void startUp() {
        if (taskList == null) {
            taskList = reader.getGlobalValue("task.list");
        }

        for (String taskName : taskList.split("[,;]+")) {
            logger.info("task[" + taskName + "] init");
            TransferTask task = genTask(taskName);
            tasks.add(task);
            Thread thread = new Thread(task);
            threads.add(thread);
            thread.start();
        }
    }

    public TransferTask genTask(String taskName) {
        Properties properties = reader.getSection(taskName);
        TransferTask task = new TransferTask(taskName, properties, defaultSrcDB, defaultDestDB);
        return task;
    }

    public void shutdown() {
        for (TransferTask task : tasks) {
            task.stop();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            }catch (InterruptedException ex){

            }
        }
    }

    public static void main(String[] args) {
        TransferController job = new TransferController("db-transfer.cfg");
        if (args.length > 0) {
            job.taskList = args[0];
        }
        job.startUp();
    }
}
