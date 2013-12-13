import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHDFS {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.addResource("/etc/hadoop/conf/core-site.xml");
		// conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
		conf.set("fs.default.name", "hdfs://localhost:8020/user/training/");

		FileSystem fsHDFS = FileSystem.get(conf);

		Path pathDest = new Path("shakespeare");

		FileStatus[] files = fsHDFS.listStatus(pathDest);
		for (FileStatus file : files) {
			System.out.println(file.getPath().getName());
		}

		Configuration local = new Configuration();
		conf.set("fs.default.name", "file:///");
		FileSystem fsLocal = FileSystem.get(local);
		Path pathLocal = new Path(
				"/home/training/training_materials/developer/data/");

		FileStatus[] filesLocal = fsLocal.listStatus(pathLocal);
		for (FileStatus file : filesLocal) {
			System.out.println(file.getPath().getName());
		}

		
	}
}
