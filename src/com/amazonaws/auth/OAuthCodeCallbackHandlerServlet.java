package com.amazonaws.auth;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.operation.servlets.GsonWriter;



/**
 * Servlet implementation class OAuthCodeCallbackHandlerServlet
 */
public class OAuthCodeCallbackHandlerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	GsonWriter gsonwrt;
	
	  
    /**
     * @see HttpServlet#HttpServlet()
     */
    public OAuthCodeCallbackHandlerServlet(){
        super();
        gsonwrt=new GsonWriter();
    }
    
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {	
		Map<String,Object> map=new HashMap<String,Object>();
		boolean isValid=false;
		
		String _atoken=request.getParameter("access_token");
		String _idtoken=request.getParameter("id_token");
		//String _expireIn=request.getParameter("expires_in");
		
		if(_atoken!=null){
			System.out.println("Token: "+_atoken);
			//System.out.println("Scope: "+_scope);
			map.put("atoken", _atoken);
			map.put("idtoken", _idtoken);
			isValid=true;
			
			/*GoogleCredential credential = new GoogleCredential()
					.setAccessToken(_atoken);			
	
			Oauth2 oauth2 = new Oauth2.Builder(HTTP_TRANSPORT,
					new JacksonFactory(), credential).setApplicationName(
					"Oauth2").build();
			Userinfoplus userinfo = oauth2.userinfo().get().execute();
			String user=userinfo.toPrettyString();*/	
			
			
			HttpSession session = request.getSession();
	            session.setAttribute("isLoggedIn", true); 
	            //session.setAttribute("user", user);
	            session.setMaxInactiveInterval(30*60);
	            
	            //_scope="Developer";
            Cookie profilec = new Cookie("user", "Dev");
            	profilec.setMaxAge(30*60);
            	
            response.addCookie(profilec);
                       
			response.sendRedirect("index.jsp");			
		}
		map.put("isValid", isValid);
		gsonwrt.write(response,map);
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
	}
	
	

}
