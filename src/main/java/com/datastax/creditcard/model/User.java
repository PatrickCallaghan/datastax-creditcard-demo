package com.datastax.creditcard.model;



public class User {
	
	private String userId;
	private String firstname;
	private String lastname;
	private String cityName;
	private String stateName;
	private String gender;
	private String creditCardNo;
	private String email;
	
	public User(){};
	
	public String getUserId() {
		return userId;
	}


	public void setUserId(String userId) {
		this.userId = userId;
	}


	public String getFirstname() {
		return firstname;
	}


	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}


	public String getLastname() {
		return lastname;
	}


	public void setLastname(String lastname) {
		this.lastname = lastname;
	}


	public String getCityName() {
		return cityName;
	}


	public void setCityName(String cityName) {
		this.cityName = cityName;
	}


	public String getStateName() {
		return stateName;
	}


	public void setStateName(String stateName) {
		this.stateName = stateName;
	}


	public String getGender() {
		return gender;
	}


	public void setGender(String gender) {
		this.gender = gender;
	}


	public String getCreditCardNo() {
		return creditCardNo;
	}


	public void setCreditCardNo(String creditCardNo) {
		this.creditCardNo = creditCardNo;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	@Override
	public String toString() {
		return "User [userId=" + userId + ", firstname=" + firstname + ", lastname=" + lastname + ", cityName="
				+ cityName + ", stateName=" + stateName + ", gender=" + gender + ", creditCardNo=" + creditCardNo
				+ ", email=" + email + "]";
	}
}
