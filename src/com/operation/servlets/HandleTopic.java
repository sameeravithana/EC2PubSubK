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
import com.google.gson.Gson;



/**
 * Servlet implementation class HandleTopic
 */
@WebServlet("/build")
public class HandleTopic extends HttpServlet {
	private static final long serialVersionUID = 1L;
    GsonWriter gsonwrt;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public HandleTopic() {
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
		String _topic=request.getParameter("_topic");
		if(_topic!=null && _topic.trim().length()!=0){
			System.out.println("Servlet: "+_topic);
			new Engine().getSns_drv().createSNSTopic(_topic);
			isValid=true;
			map.put("topic", _topic);
		}
		map.put("isValid", isValid);
		gsonwrt.write(response,map);
	}

	

}
