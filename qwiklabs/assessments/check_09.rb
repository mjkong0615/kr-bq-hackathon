# method description
def check_09(handles:, maximum_score:, resources:)
  bigquery = handles['project_0.BigqueryV2']
  ret_hash = { :score => 0, :message => "", :student_message => "" }
  isAvailable = true

  begin
    final_customer_insights_object = bigquery.get_table("cymbal".to_s, "final_customer_insights".to_s ) || []
  rescue 
    isAvailable = false
  end 

  if final_customer_insights_object
    isAvailable = true
  end 

  if isAvailable 
    success_message = "테이블이 정상적으로 생성되었습니다."
    ret_hash = { :done => true, :score => maximum_score, :message => success_message, :student_message => success_message}
  else
    error_message = "다시 한 번 절차를 확인해보세요!"
    ret_hash[:message] = error_message
    ret_hash[:student_message] = error_message
  end
  return ret_hash
end