package Enron;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class EnronPartitioner<K, V> extends Partitioner<K, V> {
	private int m=22;
	private int firstLetterValue=0;

	public int getPartition(K key, V value, int numReduceTasks) {
		// TODO override getPartition(K, V, int) method
		if(numReduceTasks!=0&&numReduceTasks!=1&&numReduceTasks!=2)
		{
			return -1;
		}
		else if(numReduceTasks==0||numReduceTasks==1)
		{
			return 0;
		}
		else if(numReduceTasks==2)
		{
			String temp=key.toString();
			int ascii=(int) temp.charAt(0);
			if(ascii>=97&&ascii<=109)
			{
				return 0;
			}
			else if(ascii>=110&&ascii<=122)
			{
				return 1;
			}
		} 
		return 0;

	}
}

