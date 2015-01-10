package BigData;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemTools {
	public FileSystemTools(){
		
	}
	
	public static boolean uploadImageToHdfs(BufferedImage srcImage, String dst, Configuration conf){
		if(dst.length() != 0){
			Path dstPath = new Path(dst);
			try{
				FileSystem fs = dstPath.getFileSystem(conf);
				FSDataOutputStream doc = fs.create(new Path(dst));
				ImageIO.write(srcImage, "jpg", doc);
				return true;
			}
			catch(IOException e){
				e.printStackTrace();
				return false;
			}
		}
		else{
			return false;
		}		
	}
	
	public static boolean putToHdfs(String src, String dst, Configuration conf) {
		if(dst.length() != 0 && src.length() != 0){
			Path dstPath = new Path(dst);
			try {
				FileSystem fs = dstPath.getFileSystem(conf);
				fs.copyFromLocalFile(false, new Path(src),new Path(dst));
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
		else{
			return false;
		}
	}
	
	public static boolean getFromHdfs(String src,String dst, Configuration conf) {
		if(src.length() != 0 && dst.length() != 0){
			Path dstPath = new Path(src);
			try {
				FileSystem fs = dstPath.getFileSystem(conf);
				fs.copyToLocalFile(false, new Path(src),new Path(dst));
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
		else{
			return false;
		}
	}
	
	public static boolean checkAndDelete(Configuration conf, final String dst){
		if(dst.length() != 0){
			Path dstPath = new Path(dst);
			try{
				//FileSystem fs = FileSystem.get(conf);
				FileSystem fs = dstPath.getFileSystem(conf);
				if(fs.exists(dstPath)){
					return fs.delete(dstPath, true);
				}
				else{
					return false;
				}
			}
			catch(IOException e){
				e.printStackTrace();
				return false;
			}
		}
		else{
			return false;
		}
	}
}