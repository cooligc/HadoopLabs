/**
 * 
 */
package com.skc.hadoop.xml2json.mapper;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author sitakant
 *
 */
public class LargeXMLGenerator {

	public static void main(String[] args) {

		ExecutorService executerService = Executors.newScheduledThreadPool(100);

		Runnable _worker = new PrepareData(1);
		executerService.execute(_worker);
		
		for (int i = 1; i <= 30; i++) {
			Runnable worker = new PrepareData(i*100);
			executerService.execute(worker); 
		}
		 

		executerService.shutdown();
		while (!executerService.isTerminated()) {
			 System.out.println("Awaiting to close all the thread");
		}

		System.out.println("All are done");
	}
}

class PrepareData implements Runnable {

	private int offset;

	public PrepareData(int offset) {
		this.offset = offset;
	}

	public void run() {
		try {
			writeToFile();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
	}

	protected void writeToFile() throws JAXBException {

		List<Employee> employeeList = new ArrayList<Employee>();
		String myData = "";
		for (int i = offset; i < offset * 100; i++) {
			Employee employee = new Employee();
			employee.setEmpId(Long.valueOf(i));
			employee.setName("cooligc" + i);
			employeeList.add(employee);
		}
		System.out.println(Thread.currentThread().getName() + "\t" + myData);

		jaxbXMLToObject(employeeList,"C:\\Users\\chaudhsi\\eclipse-workspace\\HadoopLabs\\XmlToJsonMR\\target\\"+ Thread.currentThread().getName() + "-" + UUID.randomUUID().toString() + ".xml");

	}

	private static void jaxbXMLToObject(List<Employee> employees,String fileName) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance(Employees.class);
		Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

		//jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

		Employees employees2 = new Employees();
		employees2.setEmployees(employees);
		
		// Marshal the employees list in console
		jaxbMarshaller.marshal(employees2, System.out);
		
		// Marshal the employees list in file
		jaxbMarshaller.marshal(employees2, new File(fileName));
	}

}

@XmlRootElement(name = "employee")
@XmlAccessorType(XmlAccessType.FIELD) 
class Employee implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long empId;
	private String name;

	/**
	 * @return the empId
	 */
	public Long getEmpId() {
		return empId;
	}

	/**
	 * @param empId
	 *            the empId to set
	 */
	public void setEmpId(Long empId) {
		this.empId = empId;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Employee [empId=" + empId + ", name=" + name + "]";
	}
}

@XmlRootElement(name = "employees")
@XmlAccessorType(XmlAccessType.FIELD)
class Employees implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@XmlElement(name = "employee")
	private List<Employee> employees = null;

	/**
	 * @return the employees
	 */
	public List<Employee> getEmployees() {
		return employees;
	}

	/**
	 * @param employees
	 *            the employees to set
	 */
	public void setEmployees(List<Employee> employees) {
		this.employees = employees;
	}

}
