package de.xwic.etlgine.server.admin.users;

import de.jwic.base.IControlContainer;
import de.xwic.appkit.core.config.ConfigurationException;
import de.xwic.appkit.core.model.entities.IMitarbeiter;
import de.xwic.appkit.core.model.queries.PropertyQuery;
import de.xwic.appkit.webbase.entityviewer.EntityListViewConfiguration;
import de.xwic.etlgine.server.admin.BaseContentContainer;

public class UsersListControl extends BaseContentContainer {

	public UsersListControl(IControlContainer container, String name) {
		super(container, name);
		

		setTitle("People");

		PropertyQuery baseQuery = new PropertyQuery();

		PropertyQuery defaultQuery = new PropertyQuery();
		defaultQuery.setSortField("nachname");
		defaultQuery.setSortDirection(PropertyQuery.SORT_DIRECTION_UP);

		EntityListViewConfiguration config = new EntityListViewConfiguration(IMitarbeiter.class);
		config.setBaseFilter(baseQuery);
		config.setDefaultFilter(defaultQuery);
		config.setViewId("people_all");

		try {
			new PeopleView(this, config);
		} catch (ConfigurationException e) {
			throw new RuntimeException("Can not create EntityTable: " + e, e);
		}
		
	}

}
