/**
 * source: jQuery-1.11.0
 */
$(document).ready(function() {
	$('#buildTopic').submit(function() {
		$.ajax({
			url: 'build',
			type: 'POST',
			dataType: 'json',
			data: $('#buildTopic').serialize(),
			success: function(data) {
				if(data.isValid){
					alert("Valid!");
					$('[name="_topic"]').val('');
				}else{
					alert("Invalid!");
				}
			}
		});
		return false;
	});
});

$(document).ready(function() {
	$('#subscribeTopic').submit(function() {
		$.ajax({
			url: 'subscribe',
			type: 'POST',
			dataType: 'json',
			data: $('#subscribeTopic').serialize(),
			success: function(data) {
				if(data.isValid){
					alert("Valid!");
					$('[name="_snsTopic"]').val('');
					$('[name="_snsProtocol"]').val('');
					$('[name="_snsEnd"]').val('');
					
				}else{
					alert("Invalid!");
				}
			}
		});
		return false;
	});
});

$(document).ready(function() {
	$('#publishTopic').submit(function() {
		$.ajax({
			url: 'publish',
			type: 'POST',
			dataType: 'json',
			data: $('#publishTopic').serialize(),
			success: function(data) {
				if(data.isValid){
					alert("Valid!");
					$('[name="_snsPTopic"]').val('');
					$('[name="_snsPMsg"]').val('');
					
					
				}else{
					alert("Invalid!");
				}
			}
		});
		return false;
	});
});

$(document).ready(function() {
	$('#publishStream').submit(function() {
		$.ajax({
			url: 'publishS',
			type: 'POST',
			dataType: 'json',
			data: $('#publishStream').serialize(),
			success: function(data) {
				if(data.isValid){
					$('[name="_dataurl"]').val('');
					alert("Valid!");				
					
					
				}else{
					alert("Invalid!");
				}
			}
		});
		return false;
	});
});
