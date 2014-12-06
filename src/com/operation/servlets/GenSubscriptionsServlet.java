package com.operation.servlets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.compute.Engine;
import com.pubsub.data.SubGenerator;

/**
 * Servlet implementation class GenSubscriptionsServlet
 */
@WebServlet("/generateSub")
public class GenSubscriptionsServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	GsonWriter gsonwrt;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public GenSubscriptionsServlet() {
        super();
        gsonwrt=new GsonWriter();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Map<String,Object> map=new HashMap<String,Object>();
		boolean isValid=false;
		System.out.println("Action Binded: Generating subscriptions..");
		
		ServletContext context = getServletContext();
		String contextPath = context.getRealPath("//GenSubscription//");		
		
		SubGenerator subgen=new SubGenerator(contextPath);		
		
		isValid=true;
		
		map.put("isValid", isValid);
		gsonwrt.write(response,map);
	}

	
}
