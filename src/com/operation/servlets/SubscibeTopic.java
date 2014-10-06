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
 * Servlet implementation class SubscibeTopic
 */
@WebServlet("/subscribe")
public class SubscibeTopic extends HttpServlet {
	private static final long serialVersionUID = 1L;
	GsonWriter gsonwrt;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public SubscibeTopic() {
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
		String _snsTopic=request.getParameter("_snsTopic");
	   	 String _snsProtocol=request.getParameter("_snsProtocol");
	   	 String _snsEnd=request.getParameter("_snsEnd");
		if(_snsTopic!=null && _snsProtocol!=null && _snsEnd!=null){
			System.out.println("Subscribed: "+_snsTopic+" "+_snsProtocol+" "+_snsEnd);
			new Engine().getSns_drv().subscribe(_snsTopic, _snsProtocol, _snsEnd);
			isValid=true;
			map.put("snsTopic", _snsTopic);
			map.put("snsProtocol", _snsProtocol);
			map.put("snsEnd", _snsEnd);
		}
		map.put("isValid", isValid);
		gsonwrt.write(response,map);		
   				
	}

}
