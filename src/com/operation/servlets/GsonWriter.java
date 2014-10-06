package com.operation.servlets;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;

public class GsonWriter {

	public GsonWriter() {
		// TODO Auto-generated constructor stub
	}
	
	public void write(HttpServletResponse response, Map<String, Object> map) {
		response.setContentType("application/json");
		response.setCharacterEncoding("UTF-8");
		try {
			response.getWriter().write(new Gson().toJson(map));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
