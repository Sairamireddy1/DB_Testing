package StoredFunction_Testing;

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

public class SF_Testing {
	Connection con=null;
	Statement stmt1=null;
	Statement stmt2=null;
	ResultSet rs;

	CallableStatement Cstmt;
	
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
	void test_storedfunctionExists() throws SQLException
	{
			Statement stmt=con.createStatement();
			rs= stmt.executeQuery("Show Function status where name='customerlevel'");
			rs.next();
			Assert.assertEquals(rs.getString("name"),"CustomerLevel");
//			con.createStatement().executeQuery("{Show Function status where name='customerlevel'}");
	}
	
	@Test(priority=1)
	void Test_CustomerLevel() throws SQLException
	{
		ResultSet rs1;
		ResultSet rs2;
		
		stmt1 =con.createStatement();
		stmt2 =con.createStatement();
		rs1= stmt1.executeQuery("select customerName,CustomerLevel(creditLimit) from customers");
		rs2= stmt2.executeQuery("select customerName,(case	when creditLimit>50000 then 'Platinum'   when creditLimit<=50000 and creditLimit>10000 then 'Gold'    when creditLimit < 10000 then 'Silver'end) as customerlevel from customers");
		
		Assert.assertEquals(compareResultset(rs1,rs2),true);
	}
	
	@Test(priority=2)
	void Test_GetCustomerLevelByID() throws SQLException
	{
		Cstmt = con.prepareCall("{call GetCustomerLevelByID(?,?)}");
		Cstmt.setInt(1,131);
		Cstmt.registerOutParameter(2,Types.VARCHAR);

		Cstmt.executeQuery();
		
		String Cust_Level= Cstmt.getString(2);
		
		Statement stmt=con.createStatement();
		rs= stmt.executeQuery("select customerName,\r\n"
				+ "case\r\n"
				+ "	when creditLimit>50000 then 'Platinum'\r\n"
				+ "    when creditLimit<=50000 and creditLimit>10000 then 'Gold'\r\n"
				+ "    when creditLimit < 10000 then 'Silver'\r\n"
				+ "end as customerlevel from customers where customernumber=131;");
		rs.next();
		
		String exp_Cust_Level = rs.getString("CustomerLevel");
		
		Assert.assertEquals(Cust_Level, exp_Cust_Level);
	}
	
	
	
public boolean compareResultset(ResultSet resultset1, ResultSet resultset2) throws SQLException
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
