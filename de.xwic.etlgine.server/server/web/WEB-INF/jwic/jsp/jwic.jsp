<%@ page import='de.jwic.base.*' %>
<%@ page import='de.jwic.renderer.util.*' %>
<%@ page import='java.util.*' %>
<%
	Control control = (Control)request.getAttribute("control");
	ChildRenderer insert = (ChildRenderer)request.getAttribute("insert");
	JWicTools jwic = (JWicTools)request.getAttribute("jwic");
%>