package com.tr.dbor.cdb.distribution.evmsdi;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


import org.apache.hadoop.hbase.client.ConnectionFactory;

//config/hbase-site.xml config/core-site.xml bigdata-app-commoditiesdata-pcadmin@INTQA.THOMSONREUTERS.COM config/bigdata-app-commoditiesdata-pcadmin.keytab config/krb5.conf F 20200101000000
//config/hbase-site.xml config/core-site.xml bigdata-app-commoditiesdata-pcadmin@INTQA.THOMSONREUTERS.COM config/bigdata-app-commoditiesdata-pcadmin.keytab config/krb5.conf I

public class SDIGenerator {
	public final static String configfile="config/config.properties";
	public final static String DATE_FORMAT_SSS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public final static String GENERAL_DATE_FORMAT_SSS = "yyyyMMddHHmmss";

	private static DateTimeFormatter generaldateformatter =  DateTimeFormat.forPattern(GENERAL_DATE_FORMAT_SSS).withZoneUTC();
	private static DateTimeFormatter dateformatter =  DateTimeFormat.forPattern(DATE_FORMAT_SSS).withZoneUTC();

	public static void main(String[] args) throws Exception {


		// TODO Auto-generated method stub
		String HbaseConfFile=args[0];
		String CoreConfFile=args[1];
		String titanUserID=args[2];
		String keytabFile=args[3];
		String krb5ConfFile=args[4];
		String FileType=args[5]; //For fullload, this arg could be 'F', and should be I for incremental
		
		boolean IsFull=false;

		String DataTo=""; 
		long DataToLong=0L;
		
		String DataFrom="begining";
		long DataFromLong=0L;
		
		final Configuration HbaseConf = new Configuration();
		HbaseConf.addResource(new FileInputStream(HbaseConfFile));
		HbaseConf.addResource(new FileInputStream(CoreConfFile));
		System.setProperty("java.security.krb5.conf", krb5ConfFile);
		UserGroupInformation.setConfiguration(HbaseConf); 
		UserGroupInformation.loginUserFromKeytab(titanUserID, keytabFile);
		
		Properties prop = new Properties();
		try {
			FileInputStream fis = new FileInputStream(configfile);
			prop.load(fis);
		} catch (Exception e) {
			System.out.println("Can't read configuration file: " + configfile);
		}

		String outputlocation;
		String SDIFileName;
		
		Connection connection = ConnectionFactory.createConnection(HbaseConf);
		
		Table SDIHistoryTable = connection.getTable(TableName.valueOf("commoditiesdata:cdb-series-sdi-history"));
		
		if (FileType.equals("F"))
		{
			IsFull=true;
			DataTo=args[6];//The until timestamp for full load, no this parameter for incremental
			DataToLong=DateTime.parse(DataTo, generaldateformatter).getMillis();
			
			System.out.println("Generating full SDI to extract updates until "+DataTo);
			
			outputlocation=prop.getProperty("fulloutputfolder");
			SDIFileName="CDBSeriesMeta.1."+DataTo+".Full.xml.gz";
			
			Put put = new Put(Bytes.toBytes("F"+LpadNum(Long.MAX_VALUE - DataToLong, 19)));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("runstarttime"), Bytes.toBytes(new DateTime().withZone(DateTimeZone.UTC).toString(generaldateformatter)));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("datauntiltime"), Bytes.toBytes(DataTo));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("filename"), Bytes.toBytes(SDIFileName));
			
			SDIHistoryTable.put(put);
			
		}
		else
		{
			//Figure out DataFrom and DataTo for incremental SDI
			DataToLong=new DateTime().withZone(DateTimeZone.UTC).getMillis();
			DataTo=new DateTime(DataToLong).withZone(DateTimeZone.UTC).toString(GENERAL_DATE_FORMAT_SSS);
			
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes("I"+LpadNum(0, 19)));
			scan.setStopRow(Bytes.toBytes("I"+LpadNum(Long.MAX_VALUE, 19)));
			scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("runendtime"));
			scan.setMaxResultSize(1);
			//Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,  new BinaryComparator(Bytes.toBytes("runendtime")));
			//scan.setFilter(filter);
			//Find out DataTo from the last successful incremental SDI
			ResultScanner resultscanner=SDIHistoryTable.getScanner(scan);
			
			Result res=resultscanner.next();
			
			if (res!=null)
			{
				String rowkey=Bytes.toString(res.getRow());
				DataFromLong=Long.MAX_VALUE-Long.parseLong(rowkey.substring(1));
				DataFrom=new DateTime(DataFromLong).withZone(DateTimeZone.UTC).toString(generaldateformatter);				
			}
			
			resultscanner.close();
			
			System.out.println("Generating incremental SDI to extract updates from "+DataFrom+" to "+DataTo);
			
			outputlocation=prop.getProperty("incrementaloutputfolder");
			SDIFileName="CDBSeriesMeta.1."+DataFrom+"."+DataTo+".Incremental.xml.gz";
			
			Put put = new Put(Bytes.toBytes("I"+LpadNum(Long.MAX_VALUE - DataToLong, 19)));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("runstarttime"), Bytes.toBytes(new DateTime().withZone(DateTimeZone.UTC).toString(generaldateformatter)));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("datafromtime"), Bytes.toBytes(DataFrom));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("datauntiltime"), Bytes.toBytes(DataTo));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("filename"), Bytes.toBytes(SDIFileName));
			SDIHistoryTable.put(put);
			
		}


		Table SDIDataTable = connection.getTable(TableName.valueOf("commoditiesdata:cdb-series-sdi-data"));

		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(LpadNum(Long.MAX_VALUE - DataToLong, 19)+"0000000000"));
		scan.setStopRow(Bytes.toBytes(LpadNum(Long.MAX_VALUE - DataFromLong, 19)+"9999999999"));
		scan.addFamily(Bytes.toBytes("d"));
		scan.setMaxVersions(1);
		ResultScanner resultscanner=SDIDataTable.getScanner(scan);


		File thisTempFile;
		thisTempFile = new File(outputlocation, SDIFileName + ".temp");

		// Delete temp file if it exists
		if (thisTempFile.exists() == true) {
			thisTempFile.delete();
		}

		thisTempFile.createNewFile();

		GZIPOutputStream zfile = new GZIPOutputStream(new FileOutputStream(
				thisTempFile));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(zfile, "UTF8"));

		bw.write(getHeader(IsFull));
		bw.flush();

		for (Result res:resultscanner)
		{

			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
			doc.setStrictErrorChecking(false);
			Element elContentItem = doc.createElement("env:ContentItem");
			elContentItem.setAttribute("action", "Overwrite");
			doc.appendChild(elContentItem);
			Element elData = doc.createElement("env:Data");
			elContentItem.appendChild(elData);

			Element elCPA = doc.createElement("CDBSeries");
			elData.appendChild(elCPA);

			String rowkey=Bytes.toString(res.getRow());		
			
			String Adminstatus="";
			String Effectivefrom="";
			String Effectiveto="";
			ArrayList<Element> Elements=new ArrayList<Element>();
			
			for (Cell cell : res.rawCells()) {
				String Qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String Value = Bytes.toString(CellUtil.cloneValue(cell));			

				if (Qualifier.equals("AdminStatus"))
				{
					Adminstatus=Value;
				}
				else if (Qualifier.equals("AdminStatusFrom"))
				{
					Effectivefrom=DateTime.parse(Value, generaldateformatter).toString(DATE_FORMAT_SSS);
				}
				else if (Qualifier.equals("AdminStatusTo"))
				{
					if (!Value.trim().equals(""))
					{
						Effectiveto=DateTime.parse(Value, generaldateformatter).toString(DATE_FORMAT_SSS);
					}
				}
				
				else
				{
					
					Element elementdata = doc.createElement(Qualifier);
					elementdata.appendChild(doc.createTextNode(Value.trim()));
					Elements.add(elementdata);
				}
			}
			
			Element AdminStatusElement = doc.createElement("AdminStatus");
			AdminStatusElement.appendChild(doc.createTextNode(Adminstatus.trim()));
			AdminStatusElement.setAttribute("effectiveFrom", Effectivefrom);
			if (!Effectiveto.equals(""))
			{
				AdminStatusElement.setAttribute("effectiveTo", Effectiveto);
			}
			
			elCPA.appendChild(AdminStatusElement);
			
			for (Element ele:Elements)
			{
				elCPA.appendChild(ele);
			}

			bw.write(getDocumentAsXml(doc));		
		}

		resultscanner.close();
		
		bw.write(getFooter());
		bw.flush();
		bw.close();

		File thisFile = new File(outputlocation, SDIFileName);

		// Delete temp file if it exists
		if (thisFile.exists() == true) {
			thisFile.delete();
		}

		MoveFile(thisTempFile, thisFile);
		
		if (IsFull)
		{
			
			Put put = new Put(Bytes.toBytes("F"+LpadNum(Long.MAX_VALUE - DataToLong, 19)));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("runendtime"), Bytes.toBytes(new DateTime().withZone(DateTimeZone.UTC).toString(generaldateformatter)));
			SDIHistoryTable.put(put);
			SDIHistoryTable.close();
			System.out.println("Generated full SDI to extract updates until "+DataTo);
		}
		else
		{
			Put put = new Put(Bytes.toBytes("I"+LpadNum(Long.MAX_VALUE - DataToLong, 19)));
			put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("runendtime"), Bytes.toBytes(new DateTime().withZone(DateTimeZone.UTC).toString(generaldateformatter)));
			SDIHistoryTable.put(put);
			SDIHistoryTable.close();
			
			System.out.println("Generated incremental SDI to extract updates from "+DataFrom+" to "+DataTo);
		}
		
		
		connection.close();
		
	}


	public static void MoveFile(File f1, File f2) throws Exception{

		int length = 1048576;
		FileInputStream in = new FileInputStream(f1);
		FileOutputStream out = new FileOutputStream(f2);
		byte[] buffer = new byte[length];

		while (true) {
			int ins = in.read(buffer);
			if (ins == -1) {
				in.close();
				out.flush();
				out.close();
				f1.delete();

				return;
			} else {
				out.write(buffer, 0, ins);
			}
		}
	}	

	public static String getHeader(boolean isfull)
	{
		String updatetype="Incremental";

		if (isfull)
		{
			updatetype="Full";
		}

		String uuid = UUID.randomUUID().toString();
		String strDate=new DateTime().toString(DATE_FORMAT_SSS);

		String envolope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
				+ "<env:ContentEnvelope xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\" xmlns=\"http://cdbtimeseries.schemas.financial.thomsonreuters.com/2016-10-07/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://cdbtimeseries.schemas.financial.thomsonreuters.com/2016-10-07/ CDBSeries.xsd\"  pubStyle=\""+updatetype+"\" majVers=\"3\" minVers=\"1.0\">\r\n";

		String header = envolope
				+ "	<env:Header>\r\n"
				+ "		<env:Info>\r\n"
				+ "			<env:Id>urn:uuid:"+uuid+"</env:Id>\r\n"
				+ "			<env:TimeStamp>"+strDate+"</env:TimeStamp>\r\n"
				+ "		</env:Info>\r\n"
				+ "</env:Header>\r\n"
				+ "<env:Body contentSet=\"CDBSeries\" majVers=\"2\" minVers=\"1.0\">\r\n";


		return header;		
	}


	protected static String getFooter() {
		return "</env:Body>\r\n" + "</env:ContentEnvelope>";
	}	

	public static String LpadNum(long num, int pad) {
		String res = Long.toString(num);
		if (pad > 0) {
			while (res.length() < pad) {
				res = "0" + res;
			}
		}
		return res;
	}

	public static String RpadNum(long num, int pad) {
		String res = Long.toString(num);
		if (pad > 0) {
			while (res.length() < pad) {
				res = res+"0";
			}
		}
		return res;
	}
	
	protected static String getDocumentAsXml(Document doc) throws Exception {
		
			DOMSource domSource = new DOMSource(doc);

			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
					"yes");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			// transformer.setOutputProperty(OutputKeys.ENCODING,"ISO-8859-1");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			// we want to pretty format the XML output
			// note : this is broken in jdk1.5 beta!
			transformer.setOutputProperty(
					"{http://xml.apache.org/xslt}indent-amount", "4");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			//
			java.io.StringWriter sw = new java.io.StringWriter();

			StreamResult sr = new StreamResult(sw);
			transformer.transform(domSource, sr);
			return sw.toString();

		
	}	

}
