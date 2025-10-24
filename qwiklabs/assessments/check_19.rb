# method description
def check_19(handles:, maximum_score:, resources:)
  bigquery = handles['project_0.BigqueryV2']
  ret_hash = { :score => 0, :message => "", :student_message => "" }
  isAvailable = true
  if isAvailable 
    ret_hash = { :done => true, :score => maximum_score, :message => "Assessment completed!", :student_message => "step3 completed!"}
  else
    error_message = 'Please create the BigQuery dataset.'
    ret_hash[:message] = error_message
    ret_hash[:student_message] = error_message
  end
  return ret_hash
end