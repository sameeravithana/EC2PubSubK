package com.operation.servlets;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.compute.Engine;

/**
 * Servlet implementation class ProcessStream
 */
@WebServlet("/processS")
public class ProcessStream extends HttpServlet {
	private static final long serialVersionUID = 1L;
	GsonWriter gsonwrt;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ProcessStream() {
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
		System.out.println("Action Binded: Processing Stream");
		
		new Engine().getKin_drv().startCounterApp();
		isValid=true;
		
		map.put("isValid", isValid);
		gsonwrt.write(response,map);
	}

}
