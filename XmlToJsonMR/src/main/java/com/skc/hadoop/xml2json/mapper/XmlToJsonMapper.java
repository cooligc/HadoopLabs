/**
 * 
 */
package com.skc.hadoop.xml2json.mapper;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

/**
 * @author chaudhsi
 *
 */
public class XmlToJsonMapper extends Mapper<Object, Text, NullWritable, Text> {

	final NullWritable nw = NullWritable.get();

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String xmData = value.toString();

		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(Employees.class);
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

			StringReader reader = new StringReader(xmData);
			Employees employees = (Employees) unmarshaller.unmarshal(reader);

			Gson gson = new Gson();
			String output = gson.toJson(employees);
			context.write(nw, new Text(output));

		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
