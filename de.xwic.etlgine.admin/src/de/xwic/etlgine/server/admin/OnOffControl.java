package de.xwic.etlgine.server.admin;

import java.util.List;

import de.jwic.base.ControlContainer;
import de.jwic.base.IControlContainer;
import de.jwic.controls.Button;
import de.jwic.controls.Label;
import de.jwic.controls.layout.TableLayoutContainer;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.publish.CubePublishDestination;
import de.xwic.etlgine.publish.CubePublisherManager;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.ServerContext;

public class OnOffControl extends ControlContainer {

	public OnOffControl(IControlContainer container, String name) {
		super(container, name);
		TableLayoutContainer table = new TableLayoutContainer(this, "table");
		table.setColumnCount(3);
		table.setColWidth(0, 400);
		table.setColWidth(1, 150);
		table.setColWidth(2, 150);

		new Label(table).setText("Notifications:");
		// Activate trigger
		Button btEnableNOTF = new Button(table, "btEnableNOTF");
		btEnableNOTF.setTitle("On");
		btEnableNOTF.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
				serverContext.setProperty("notifications.enabled", "true");
			}
		});

		Button btDisableNOTF = new Button(table, "btDisableNOTF");
		btDisableNOTF.setTitle("Off");
		btDisableNOTF.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
				serverContext.setProperty("notifications.enabled", "false");
			}
		});

		new Label(table).setText("Trigger:");
		// Activate trigger
		Button btEnableTRG = new Button(table, "btEnableTRG");
		btEnableTRG.setTitle("On");
		btEnableTRG.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
				serverContext.setProperty("trigger.enabled", "true");
			}
		});

		Button btDisableTRG = new Button(table, "btDisableTRG");
		btDisableTRG.setTitle("Off");
		btDisableTRG.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
				serverContext.setProperty("trigger.enabled", "false");
			}
		});

		if (null != getPublishDestinations()) {
			for (final CubePublishDestination cubePublishDestination : getPublishDestinations()) {
				new Label(table).setText(cubePublishDestination.getFullKey() + ":");
				Button btPublishEnable = new Button(table, "btPublishEnable_" + cubePublishDestination.getFullKey());
				btPublishEnable.setTitle("On");
				btPublishEnable.addSelectionListener(new SelectionListener() {
					public void objectSelected(SelectionEvent event) {
						setPublishStatus(cubePublishDestination.getFullKey(), true);
					}
				});

				Button btPublishDisable = new Button(table, "btPublishDisable_" + cubePublishDestination.getFullKey());
				btPublishDisable.setTitle("Off");
				btPublishDisable.addSelectionListener(new SelectionListener() {
					public void objectSelected(SelectionEvent event) {
						setPublishStatus(cubePublishDestination.getFullKey(), false);
					}
				});
			}
		}

		ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
		String cloning = serverContext.getProperty("ps.proxy.cloning.mode");

		if (null != cloning && !cloning.isEmpty() && !cloning.equals("0")) {
			new Label(table).setText("CloningMode:");

			Button btEnableCLN = new Button(table, "btEnableCLN");
			btEnableCLN.setTitle("On");
			btEnableCLN.addSelectionListener(new SelectionListener() {
				public void objectSelected(SelectionEvent event) {
					ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
					serverContext.setProperty("ps.proxy.cloning.mode", cloning);
				}
			});

			Button btDisableCLN = new Button(table, "btDisableCLN");
			btDisableCLN.setTitle("Off");
			btDisableCLN.addSelectionListener(new SelectionListener() {
				public void objectSelected(SelectionEvent event) {
					ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
					serverContext.setProperty("ps.proxy.cloning.mode", "0");
				}
			});
		}

	}

	public boolean showEnable() {
		return ETLgineServer.getInstance().getServerContext().getPropertyBoolean("trigger.enabled", true);
	}

	public List<CubePublishDestination> getPublishDestinations() {
		return CubePublisherManager.getInstance().getPublishTargets();
	}

	public void setPublishStatus(String targetKey, boolean publishEnabled) {
		CubePublisherManager.getInstance().setTargetEnabled(targetKey, publishEnabled);
	}

}
