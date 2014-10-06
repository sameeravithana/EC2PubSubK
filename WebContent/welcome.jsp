<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@ page import = "java.util.ResourceBundle" %>


<!DOCTYPE html>
<html lang="en">

<head>

    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="description" content="Research project">
    <meta name="author" content="Y.S.Horawalavithana">

    <title>Cloud Top-k Publish/Subscribe DevCenter</title>

    <!-- Bootstrap Core CSS -->
    <link href="css/bootstrap.min.css" rel="stylesheet">

    <!-- Custom CSS -->
    <link href="css/landing-page.css" rel="stylesheet">

    <!-- Custom Fonts -->
    <link href="font-awesome-4.1.0/css/font-awesome.min.css" rel="stylesheet" type="text/css">
    <link href="http://fonts.googleapis.com/css?family=Lato:300,400,700,300italic,400italic,700italic" rel="stylesheet" type="text/css">

    <!-- HTML5 Shim and Respond.js IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
    <!--[if lt IE 9]>
        <script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
        <script src="https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js"></script>
    <![endif]-->

	<script type="text/javascript" src="js/jquery-1.11.0.js"></script>
	
</head>

<body>

    <!-- Navigation -->
    <nav class="navbar navbar-default navbar-fixed-top" role="navigation">
        <div class="container">
            <!-- Brand and toggle get grouped for better mobile display -->
            <div class="navbar-header">
                <button type="button" class="navbar-toggle" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1">
                    <span class="sr-only">Toggle navigation</span>
                    <span class="icon-bar"></span>
                    <span class="icon-bar"></span>
                    <span class="icon-bar"></span>
                </button>
                <a class="navbar-brand" href="#">DevCenter</a>
            </div>
            <!-- Collect the nav links, forms, and other content for toggling -->
            <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
                <ul class="nav navbar-nav navbar-right">
                    <li>
                        <a href="#">About</a>
                    </li>
                    <li>
                        <a href="#">Services</a>
                    </li>
                    <li>
                        <a href="#">Contact</a>
                    </li>
                </ul>
            </div>
            <!-- /.navbar-collapse -->
        </div>
        <!-- /.container -->
    </nav>

    <!-- Header -->
    <div class="intro-header">

        <div class="container">

            <div class="row">
                <div class="col-lg-12">
                    <div class="intro-message">
                        <h1>Cloud Top-k Publish/Subscribe DevCenter</h1>                        
                        <hr class="intro-divider">
                        <ul class="list-inline intro-social-buttons">
                            <!-- <li>
                                <a href="https://twitter.com/SBootstrap" class="btn btn-default btn-lg"><i class="fa fa-twitter fa-fw"></i> <span class="network-name">Twitter</span></a>
                            </li> -->
                            <li>
                                <a href="" class="btn btn-default btn-lg"><i class="fa fa-github fa-fw"></i> <span class="network-name">Github</span></a>
                            </li>
                            <li>
                            	
                                <a href="#" class="btn btn-danger btn-lg"> 
                                
	                                <span id="signinButton">
									  <span
									    class="g-signin"
									    data-callback="signinCallback"
									    <% ResourceBundle resource = ResourceBundle.getBundle("AwsCredentials"); 
									    	String gclient=resource.getString("gclient"); 
									    %>
									    data-clientid="<%= gclient %>"
									    data-cookiepolicy="single_host_origin"
									    data-requestvisibleactions="http://schema.org/AddAction"
									    data-scope="https://www.googleapis.com/auth/plus.login">
									  </span>
									</span>
                                </a>
                                
                                <script type="text/javascript">
	
									function signinCallback(authResult) {
										if (authResult['status']['signed_in']) {
											// Update the app to reflect a signed in user
											// Hide the sign-in button now that the user is authorized, for example:
											document.getElementById('signinButton').setAttribute('style',
													'display: none');
											var _access_token=authResult['access_token'];
											var _id_token=authResult['id_token'];
											var _expires_in=authResult['expires_in'];
											
											/* $.ajax({
											    url: 'oauth2callback',
											    data: {
											    	access_token: _access_token,
											    	id_token: _id_token,
											    	expires_in: _expires_in
											    },
											    type: 'GET'
											});​ */  
											//console.log('Access Token: '+_access_token);
											//window.location = "/oauth2callback?access_token="+_access_token+"&id_token="+_id_token+"&expires_in="+_expires_in;
											window.location = "/EC2PubSub/oauth2callback?access_token="+_access_token+"&id_token="+_id_token+"&expires_in="+_expires_in;
										} else {
											// Update the app to reflect a signed out user
											// Possible error values:
											//   "user_signed_out" - User is signed-out
											//   "access_denied" - User denied access to your app
											//   "immediate_failed" - Could not automatically log in the user
											console.log('Sign-in state: ' + authResult['error']);
										}
									}
								</script>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>

        </div>
        <!-- /.container -->

    </div>
    <!-- /.intro-header -->

    <!-- Page Content -->

    <div class="content-section-a">

        <div class="container">

            <div class="row">
                <div class="col-lg-5 col-sm-6">
                    <hr class="section-heading-spacer">
                    <div class="clearfix"></div>
                    <h2 class="section-heading">Amazon Kinesis:<br>Real-time Streaming Big data Processing</h2>
                    <p class="lead"> Our platform is supported by Amazon Kinesis, where publishers can put data into Amazon Kinesis streams, which ensures durability and elasticity. You can create streaming map-reduce type applications, and the elasticity of Amazon Kinesis enables you to scale the stream up or down, so that you never lose data records prior to their expiration.</p>
                </div>
                <div class="col-lg-5 col-lg-offset-2 col-sm-6">
                    <iframe src="//www.slideshare.net/slideshow/embed_code/28421498" width="427" height="356" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="https://www.slideshare.net/AmazonWebServices/amazon-kinesis-realtime-streaming-big-data-processing-applications-bdt311-aws-reinvent-2013" title="Amazon Kinesis: Real-time Streaming Big data Processing Applications (BDT311) | AWS re:Invent 2013" target="_blank">Amazon Kinesis: Real-time Streaming Big data Processing Applications (BDT311) | AWS re:Invent 2013</a> </strong> from <strong><a href="http://www.slideshare.net/AmazonWebServices" target="_blank">Amazon Web Services</a></strong> </div>
                </div>
            </div>

        </div>
        

    </div>
   

     <div class="content-section-b">

        <div class="container">

            <div class="row">
                <div class="col-lg-5 col-lg-offset-1 col-sm-6">                    
                    <img class="img-responsive" src="img/AWS_Services.png" alt="AWS Services"/>                
                </div>
                <div class="col-lg-5 col-sm-6">
                    <img class="img-responsive" src="img/intro-bg.png" alt="">
                </div>
            </div>

        </div>
        

    </div>
   

    <!--<div class="content-section-a">

        <div class="container">

            <div class="row">
                <div class="col-lg-5 col-sm-6">
                    <hr class="section-heading-spacer">
                    <div class="clearfix"></div>
                    <h2 class="section-heading">Google Web Fonts and<br>Font Awesome Icons</h2>
                    <p class="lead">This template features the 'Lato' font, part of the <a target="_blank" href="http://www.google.com/fonts">Google Web Font library</a>, as well as <a target="_blank" href="http://fontawesome.io">icons from Font Awesome</a>.</p>
                </div>
                <div class="col-lg-5 col-lg-offset-2 col-sm-6">
                    <img class="img-responsive" src="img/phones.png" alt="">
                </div>
            </div>

        </div>
        

    </div>
    

    <div class="banner">

        <div class="container">

            <div class="row">
                <div class="col-lg-6">
                    <h2>Connect to Start Bootstrap:</h2>
                </div>
                <div class="col-lg-6">
                    <ul class="list-inline banner-social-buttons">
                        <li>
                            <a href="https://twitter.com/SBootstrap" class="btn btn-default btn-lg"><i class="fa fa-twitter fa-fw"></i> <span class="network-name">Twitter</span></a>
                        </li>
                        <li>
                            <a href="https://github.com/IronSummitMedia/startbootstrap" class="btn btn-default btn-lg"><i class="fa fa-github fa-fw"></i> <span class="network-name">Github</span></a>
                        </li>
                        <li>
                            <a href="#" class="btn btn-default btn-lg"><i class="fa fa-linkedin fa-fw"></i> <span class="network-name">Linkedin</span></a>
                        </li>
                    </ul>
                </div>
            </div>

        </div>
        

    </div> -->
    

    <!-- Footer -->
    <footer>
        <div class="container">
            <div class="row">
                <div class="col-lg-12">
                    <ul class="list-inline">
                        <li>
                            <a href="#home">Home</a>
                        </li>
                        <li class="footer-menu-divider">&sdot;</li>
                        <li>
                            <a href="#about">About</a>
                        </li>
                        <li class="footer-menu-divider">&sdot;</li>
                        <li>
                            <a href="#services">Services</a>
                        </li>
                        <li class="footer-menu-divider">&sdot;</li>
                        <li>
                            <a href="#contact">Contact</a>
                        </li>
                    </ul>
                    <p class="copyright text-muted small">Copyright &copy; UCSC 2014. All Rights Reserved</p>
                </div>
            </div>
        </div>
    </footer>
    
    <!-- Place this asynchronous JavaScript just before your </body> tag -->
    <script type="text/javascript">
      (function() {
       var po = document.createElement('script'); po.type = 'text/javascript'; po.async = true;
       po.src = 'https://apis.google.com/js/client:plusone.js';
       var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(po, s);
     })();
    </script>

    <!-- jQuery Version 1.11.0 -->
    <script src="js/jquery-1.11.0.js"></script>

    <!-- Bootstrap Core JavaScript -->
    <script src="js/bootstrap.min.js"></script>

</body>

</html>

