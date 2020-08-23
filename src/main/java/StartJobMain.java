import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.ElMaper;
import utils.ElReducer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class StartJobMain extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		//replace "hive" here with the name of the user the queries should run as
		Connection con = null;
		try {
			con = DriverManager.getConnection("jdbc:hive2://25.21.32.30:10000/ViajesDomesticosCR", "", "");
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		try {
			stmt.execute("use viajesdomesticoscr;" +
					"FROM compras CMP" +
					"GROUP BY CMP.origin, CMP.destination" +
					"INSERT OVERWRITE TABLE compras_hive" +
					"SELECT" +
					"CMP.origin origen," +
					"CMP.destination destino," +
					"CMP.date fecha," +
					"CMP.rate monto;" +
					"FROM busquedas CMP" +
					"GROUP BY CMP.origin, CMP.destination" +
					"INSERT OVERWRITE TABLE busquedas_hive" +
					"SELECT" +
					"CMP.origin origen," +
					"CMP.destination destino," +
					"CMP.date fecha;");
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		try {
			con.close();
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		args = new String[3];
		args[0] = "Suma de montos por dias del mes por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/busquedas_hive";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaBusquedas";
		int res = ToolRunner.run(new StartJobMain(), args);
		args = new String[3];
		args[0] = "Suma de montos por dias del mes por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/compras_hive";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaCompras";
		res = ToolRunner.run(new StartJobMain(), args);
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		//replace "hive" here with the name of the user the queries should run as
		con = null;
		try {
			con = DriverManager.getConnection("jdbc:hive2://25.21.32.30:10000/ViajesDomesticosCR", "", "");
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		try {
			stmt.execute("use viajesdomesticoscr;" +
					"LOAD DATA INPATH '/user/hive/warehouse/viajesdomesticoscr.db/sumaBusquedas" +
					"' OVERWRITE INTO TABLE busquedasXrutaXmes_tmp;" +
					"set hive.exec.dynamic.partition.mode=nonstrict;" +
					"FROM busquedasXrutaXmes_tmp tmp" +
					"INSERT OVERWRITE TABLE busquedasXrutaXmes" +
					"SELECT" +
					"tmp.origen origen," +
					"tmp.destino destino," +
					"tmp.fecha fecha," +
					"tmp.cantidad cantidad;" +
					"LOAD DATA INPATH '/user/hive/warehouse/viajesdomesticoscr.db/sumaCompras" +
					"' OVERWRITE INTO TABLE comprasXrutaXmes_tmp;" +
					"set hive.exec.dynamic.partition.mode=nonstrict;" +
					"FROM comprasXrutaXmes_tmp tmp" +
					"INSERT OVERWRITE TABLE comprasXrutaXmes" +
					"SELECT" +
					"tmp.origen origen," +
					"tmp.destino destino," +
					"tmp.fecha fecha," +
					"tmp.cantidad cantidad;" +
					"CREATE TABLE Demanda(" +
					"origen string," +
					"destino string," +
					"fecha string," +
					"demanda float" +
					")" +
					"ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';" +
					"INSERT OVERWRITE TABLE Demanda(origen,destino,fecha,demanda)" +
					"select compras.origen,compras.destino,compras.fecha,(compras.cantidad*compras.cantidad)/(busquedas.cantidad*300)" +
					"from ComprasXrutaXmes compras" +
					"inner join busquedasXrutaXmes busquedas" +
					"on (compras.origen = busquedas.origen and compras.destino = busquedas.destino and compras.fecha = busquedas.fecha)" +
					"group by compras.origen,compras.destino,compras.fecha" +
					"order by compras.origen,compras.destino,compras.fecha");
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		try {
			con.close();
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
	}


	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), args[0]);
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(ElMaper.class);
		job.setReducerClass(ElReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}