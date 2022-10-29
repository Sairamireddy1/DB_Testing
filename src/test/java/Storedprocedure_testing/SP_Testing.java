package Storedprocedure_testing;


import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SP_Testing {

	Connection con=null;
	Statement stmt=null;
	ResultSet rs;
	CallableStatement Cstmt;
	ResultSet rs1;
	ResultSet rs2;
	
	
	@BeforeClass
	void setup() throws SQLException
	{
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
	}
	
	@AfterClass
	void teardown() throws SQLException
	{
		con.close();
	}
	
	@Test(priority=0)
	void test_storedproceduresExists() throws SQLException
	{
		stmt=con.createStatement();
		rs= stmt.executeQuery("show procedure status where name='selectallcustomers'");
		rs.next();
		Assert.assertEquals(rs.getString("Name"),"selectAllCustomers");
	}
	
	@Test(priority=1)
	void test_AllCostomers() throws SQLException
	{
		Cstmt = con.prepareCall("{call selectAllCustomers()}");
		rs1= Cstmt.executeQuery();
		
		Statement stmt=con.createStatement();
		rs2= stmt.executeQuery("select * from customers");
	
		Assert.assertEquals(compareResultsets(rs1,rs2), true);
	}
	
	@Test(priority=2)
	void Test_SelectAllCustomersByCity() throws SQLException
	{
		Cstmt = con.prepareCall("{call SelectAllCustomersByCity(?)}");
		Cstmt.setString(1,"San Francisco");
		rs1= Cstmt.executeQuery();
		
		Statement stmt=con.createStatement();
		rs2= stmt.executeQuery("select * from customers where city ='San Francisco'");
	
		Assert.assertEquals(compareResultsets(rs1,rs2), true);
	}
	
	@Test(priority=3)
	void Test_SelectAllCustomersByCityAndPin() throws SQLException
	{
		Cstmt = con.prepareCall("{call SelectAllCustomersByCityAndPin(?,?)}");
		Cstmt.setString(1,"San Francisco");
		Cstmt.setString(2,"94217");
		rs1= Cstmt.executeQuery();
		
		Statement stmt=con.createStatement();
		rs2= stmt.executeQuery("select * from customers where city ='San Francisco' and postalCode='94217'");
	
		Assert.assertEquals(compareResultsets(rs1,rs2), true);
	}
	
	@Test(priority=4)
	void Test_get_order_by_customer() throws SQLException
	{
		Cstmt = con.prepareCall("{call get_order_by_customer(?,?,?,?,?)}");
		Cstmt.setInt(1,141);
		Cstmt.registerOutParameter(2,Types.INTEGER);
		Cstmt.registerOutParameter(3,Types.INTEGER);
		Cstmt.registerOutParameter(4,Types.INTEGER);
		Cstmt.registerOutParameter(5,Types.INTEGER);

		Cstmt.executeQuery();
		
		int shipped= Cstmt.getInt(2);
		int cancelled= Cstmt.getInt(3);
		int resolved= Cstmt.getInt(4);
		int disputed= Cstmt.getInt(5);
		
		Statement stmt=con.createStatement();
		rs= stmt.executeQuery("select(select count(*) as 'shipped' from orders where customerNumber =141 and status='shipped')as shipped,(select count(*) as 'cancelled' from orders where customerNumber =141 and status='cancelled')as cancelled,(select count(*) as 'resolved' from orders where customerNumber =141 and status='resolved')as resolved,(select count(*) as 'disputed' from orders where customerNumber = 141 and status= 'disputed')as disputed;");
		
		rs.next();
		
		int exp_shipped = rs.getInt("shipped");
		int exp_cancelled = rs.getInt("cancelled");
		int exp_resolved = rs.getInt("resolved");
		int exp_disputed = rs.getInt("disputed");

		if(shipped==exp_shipped && cancelled==exp_cancelled && resolved==exp_resolved && disputed==exp_disputed)
		Assert.assertTrue(true);
		else
			Assert.assertTrue(false);
	}
	
	@Test(priority=5)
	void Test_GetCustomerSipping() throws SQLException
	{
		Cstmt = con.prepareCall("{call GetCustomerShipping(?,?)}");
		Cstmt.setInt(1,141);
		Cstmt.registerOutParameter(2,Types.VARCHAR);

		Cstmt.executeQuery();
		
		String ShippingTime= Cstmt.getString(2);
		
		Statement stmt=con.createStatement();
		rs= stmt.executeQuery("select country,\r\n"
				+ "case\r\n"
				+ "	when country='usa' then '2-day shipping'\r\n"
				+ "    when country='canada' then '3-day shipping'\r\n"
				+ "    else '5-day shipping'\r\n"
				+ "end as ShippingTime\r\n"
				+ "from customers where customerNumber=141;");
		rs.next();
		
		String exp_ShippingTime = rs.getString("ShippingTime");
		
		Assert.assertEquals(ShippingTime, exp_ShippingTime);
	}
	
	
//	Reusable Method
	public boolean compareResultsets(ResultSet resultset1, ResultSet resultset2) throws SQLException
	{
	while(resultset1.next())
	{
		resultset2.next();
		int count = resultset1.getMetaData().getColumnCount();
		for(int i=1;i<=count;i++)
		{
			if(!StringUtils.equals(resultset1.getString(i),resultset2.getString(i)))
			{
				return false;
			}
		}
	}
	return true;
	}
	
}







