package models;

import java.util.Date;

import javax.json.JsonObject;

public class EntityFieldValue {
	public int id;
	public int organization_id;
	public int branch_id;
	public int entity_field_id;
	public int record_id;
	public String value;
	public JsonObject value_json;
	public int created_by;
	public int updated_by;
	public int deleted_by;
	public Date created_at;
	public Date updated_at;
	public Date deleted_at;
}
