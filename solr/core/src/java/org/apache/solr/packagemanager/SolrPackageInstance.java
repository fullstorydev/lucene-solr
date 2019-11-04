package org.apache.solr.packagemanager;

import java.util.List;
import java.util.Map;

import org.apache.solr.packagemanager.SolrPackage.Plugin;

/**
 * Describes one instance of a package as it exists in Solr when installed.
 */
public class SolrPackageInstance {
	final public String name;
	final public String description;
	final public String version;
	final public List<Plugin> plugins;
	final Map<String, String> parameterDefaults;
	
	public SolrPackageInstance(String id, String description, String version,
	    List<Plugin> plugins, Map<String, String> params) {
		this.name = id;
		this.description = description;
		this.version = version;
		this.plugins = plugins;
		this.parameterDefaults = params;
	}

	public String getPackageName() {
		return name;
	}

	public String getPackageDescription() {
		return description;
	}

	public String getVersion() {
		return version;
	}

	public List<Plugin> getPlugins() {
    return plugins;
  }
}
