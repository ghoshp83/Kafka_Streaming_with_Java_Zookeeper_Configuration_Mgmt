package com.pralay.cm.managed;

import java.util.Set;

/**
 * Managing Resource ownership between appsuites
 *
 */
public interface AppSuiteResources {

    public static final String RESOURCE_TYPE_KAFKA = "kafka";

    /**
     * Assigns a resource to an appsuite
     *
     * @param resourceType
     * @param resourceId
     * @param appsuite
     *
     * @throws com.pralay.cm.ConfigurationException
     */
    void assignToAppSuite(String resourceType, String resourceId, String appsuite);

    /**
     * Detaches resource from appsuite
     *
     * @param resourceType
     * @param resourceId
     * @param appsuite
     */
    void detachFromAppSuite(String resourceType, String resourceId, String appsuite);

    Set<AppSuiteResource> getAllDetached();

    Set<AppSuiteResource> getResources();

    Set<AppSuiteResource> getResources(String appsuite);

    Set<String> getResources(String appsuite, String resourceType);

    public final static class AppSuiteResource {
        private final String resourceType;

        private final String resourceId;

        private String appsuite = null;

        public AppSuiteResource(String resourceType, String resourceId) {
            this.resourceType = resourceType;
            this.resourceId = resourceId;
        }

        public AppSuiteResource(String resourceType, String resourceId, String appsuite) {
            this(resourceType, resourceId);
            this.appsuite = appsuite;
        }

        public String getResourceType() {
            return resourceType;
        }

        public String getResourceId() {
            return resourceId;
        }

        public String getAppSuite() {
            return appsuite;
        }

        public void setAppSuite(String appsuite) {
            this.appsuite = appsuite;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            else if (obj instanceof AppSuiteResource) {
                AppSuiteResource other = (AppSuiteResource) obj;
                return getResourceId().equals(other.getResourceId()) &&
                        getResourceType().equals(other.getResourceType()) &&
                        ((appsuite == null && other.appsuite == null) || (appsuite != null && appsuite.equals(other.appsuite)));
            } else
                return false;
        }

        @Override
        public int hashCode() {
            return resourceId.hashCode();
        }
    }
}
